/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is the Netscape security libraries.
 *
 * The Initial Developer of the Original Code is
 * Netscape Communications Corporation.
 * Portions created by the Initial Developer are Copyright (C) 1994-2000
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */


#include "common/config.h"

#ifdef USE_NSS

#include <nspr.h>
#include <cert.h>
#include <nss.h>
#include <smime.h>

#endif

#include <string.h>
#include <errno.h>


#include "include/buffer.h"

#include "common/debug.h"

#include "ceph_crypto_cms.h"

#define dout_subsys ceph_subsys_crypto


#ifndef USE_NSS

int ceph_decode_cms(CephContext *cct, bufferlist& cms_bl, bufferlist& decoded_bl)
{
  return -ENOTSUP;
}

#else


static int cms_verbose = 0;

static SECStatus
DigestFile(PLArenaPool *poolp, SECItem ***digests, SECItem *input,
           SECAlgorithmID **algids)
{
    NSSCMSDigestContext *digcx;
    SECStatus rv;

    digcx = NSS_CMSDigestContext_StartMultiple(algids);
    if (digcx == NULL)
	return SECFailure;

    NSS_CMSDigestContext_Update(digcx, input->data, input->len);

    rv = NSS_CMSDigestContext_FinishMultiple(digcx, poolp, digests);
    return rv;
}


struct optionsStr {
    SECCertUsage certUsage;
    CERTCertDBHandle *certHandle;
};

struct decodeOptionsStr {
    struct optionsStr *options;
    SECItem            content;
    int headerLevel;
    PRBool suppressContent;
    NSSCMSGetDecryptKeyCallback dkcb;
    PK11SymKey *bulkkey;
    PRBool      keepCerts;
};

static NSSCMSMessage *
decode(CephContext *cct, SECItem *input, const struct decodeOptionsStr *decodeOptions, bufferlist& out)
{
    NSSCMSDecoderContext *dcx;
    SECStatus rv;
    NSSCMSMessage *cmsg;
    int nlevels, i;
    SECItem sitem;
    bufferptr bp;
    SECItem *item;

    memset(&sitem, 0, sizeof(sitem));

    PORT_SetError(0);
    dcx = NSS_CMSDecoder_Start(NULL, 
                               NULL, NULL,         /* content callback     */
                               NULL, NULL,         /* password callback    */
			       decodeOptions->dkcb, /* decrypt key callback */
                               decodeOptions->bulkkey);
    if (dcx == NULL) {
	ldout(cct, 0) << "ERROR: failed to set up message decoder" << dendl;
	return NULL;
    }
    rv = NSS_CMSDecoder_Update(dcx, (char *)input->data, input->len);
    if (rv != SECSuccess) {
	ldout(cct, 0) << "ERROR: failed to decode message" << dendl;
	NSS_CMSDecoder_Cancel(dcx);
	return NULL;
    }
    cmsg = NSS_CMSDecoder_Finish(dcx);
    if (cmsg == NULL) {
	ldout(cct, 0) << "ERROR: failed to decode message" << dendl;
	return NULL;
    }

    if (decodeOptions->headerLevel >= 0) {
	ldout(cct, 20) << "SMIME: " << dendl;
    }

    nlevels = NSS_CMSMessage_ContentLevelCount(cmsg);
    for (i = 0; i < nlevels; i++) {
	NSSCMSContentInfo *cinfo;
	SECOidTag typetag;

	cinfo = NSS_CMSMessage_ContentLevel(cmsg, i);
	typetag = NSS_CMSContentInfo_GetContentTypeTag(cinfo);

	ldout(cct, 20) << "level=" << decodeOptions->headerLevel << "." << nlevels - i << dendl;

	switch (typetag) {
	case SEC_OID_PKCS7_SIGNED_DATA:
	  {
	    NSSCMSSignedData *sigd = NULL;
	    SECItem **digests;
	    int nsigners;
	    int j;

	    if (decodeOptions->headerLevel >= 0)
		ldout(cct, 20) << "type=signedData; " << dendl;
	    sigd = (NSSCMSSignedData *)NSS_CMSContentInfo_GetContent(cinfo);
	    if (sigd == NULL) {
		ldout(cct, 0) << "ERROR: signedData component missing" << dendl;
		goto loser;
	    }

	    /* if we have a content file, but no digests for this signedData */
	    if (decodeOptions->content.data != NULL && 
	        !NSS_CMSSignedData_HasDigests(sigd)) {
		PLArenaPool     *poolp;
		SECAlgorithmID **digestalgs;

		/* detached content: grab content file */
		sitem = decodeOptions->content;

		if ((poolp = PORT_NewArena(1024)) == NULL) {
		    ldout(cct, 0) << "ERROR: Out of memory" << dendl;
		    goto loser;
		}
		digestalgs = NSS_CMSSignedData_GetDigestAlgs(sigd);
		if (DigestFile (poolp, &digests, &sitem, digestalgs) 
		      != SECSuccess) {
		    ldout(cct, 0) << "ERROR: problem computing message digest" << dendl;
		    PORT_FreeArena(poolp, PR_FALSE);
		    goto loser;
		}
		if (NSS_CMSSignedData_SetDigests(sigd, digestalgs, digests) 
		    != SECSuccess) {
		    ldout(cct, 0) << "ERROR: problem setting message digests" << dendl;
		    PORT_FreeArena(poolp, PR_FALSE);
		    goto loser;
		}
		PORT_FreeArena(poolp, PR_FALSE);
	    }

	    /* import the certificates */
	    if (NSS_CMSSignedData_ImportCerts(sigd, 
	                                   decodeOptions->options->certHandle, 
	                                   decodeOptions->options->certUsage, 
	                                   decodeOptions->keepCerts) 
	          != SECSuccess) {
		ldout(cct, 0) << "ERROR: cert import failed" << dendl;
		goto loser;
	    }

	    /* find out about signers */
	    nsigners = NSS_CMSSignedData_SignerInfoCount(sigd);
	    if (decodeOptions->headerLevel >= 0)
		ldout(cct, 20) << "nsigners=" << nsigners << dendl;
	    if (nsigners == 0) {
		/* Might be a cert transport message
		** or might be an invalid message, such as a QA test message
		** or a message from an attacker.
		*/
		SECStatus rv;
		rv = NSS_CMSSignedData_VerifyCertsOnly(sigd, 
		                            decodeOptions->options->certHandle, 
		                            decodeOptions->options->certUsage);
		if (rv != SECSuccess) {
		    ldout(cct, 0) << "ERROR: Verify certs-only failed!" << dendl;
		    goto loser;
		}
		return cmsg;
	    }

	    /* still no digests? */
	    if (!NSS_CMSSignedData_HasDigests(sigd)) {
		ldout(cct, 0) << "ERROR: no message digests" << dendl;
		goto loser;
	    }

	    for (j = 0; j < nsigners; j++) {
		const char * svs;
		NSSCMSSignerInfo *si;
		NSSCMSVerificationStatus vs;
		SECStatus bad;

		si = NSS_CMSSignedData_GetSignerInfo(sigd, j);
		if (decodeOptions->headerLevel >= 0) {
		    char *signercn;
		    static char empty[] = { "" };

		    signercn = NSS_CMSSignerInfo_GetSignerCommonName(si);
		    if (signercn == NULL)
			signercn = empty;
		    ldout(cct, 20) << "\t\tsigner" << j << ".id=" << signercn << dendl;
		    if (signercn != empty)
		        PORT_Free(signercn);
		}
		bad = NSS_CMSSignedData_VerifySignerInfo(sigd, j, 
		                           decodeOptions->options->certHandle, 
		                           decodeOptions->options->certUsage);
		vs  = NSS_CMSSignerInfo_GetVerificationStatus(si);
		svs = NSS_CMSUtil_VerificationStatusToString(vs);
		if (decodeOptions->headerLevel >= 0) {
		    ldout(cct, 20) << "signer" << j << "status=" << svs << dendl;
		    /* goto loser ? */
		} else if (bad) {
		    ldout(cct, 0) << "ERROR: signer " << j << " status = " << svs << dendl;
		    goto loser;
		}
	    }
	  }
	  break;
	case SEC_OID_PKCS7_ENVELOPED_DATA:
	  {
	    NSSCMSEnvelopedData *envd;
	    if (decodeOptions->headerLevel >= 0)
		ldout(cct, 20) << "type=envelopedData; " << dendl;
	    envd = (NSSCMSEnvelopedData *)NSS_CMSContentInfo_GetContent(cinfo);
	    if (envd == NULL) {
		ldout(cct, 0) << "ERROR: envelopedData component missing" << dendl;
		goto loser;
	    }
	  }
	  break;
	case SEC_OID_PKCS7_ENCRYPTED_DATA:
	  {
	    NSSCMSEncryptedData *encd;
	    if (decodeOptions->headerLevel >= 0)
		ldout(cct, 20) << "type=encryptedData; " << dendl;
	    encd = (NSSCMSEncryptedData *)NSS_CMSContentInfo_GetContent(cinfo);
	    if (encd == NULL) {
		ldout(cct, 0) << "ERROR: encryptedData component missing" << dendl;
		goto loser;
	    }
	  }
	  break;
	case SEC_OID_PKCS7_DATA:
	    if (decodeOptions->headerLevel >= 0)
		ldout(cct, 20) << "type=data; " << dendl;
	    break;
	default:
	    break;
	}
    }

    item = (sitem.data ? &sitem : NSS_CMSMessage_GetContent(cmsg));
    out.append((char *)item->data, item->len);
    return cmsg;

loser:
    if (cmsg)
	NSS_CMSMessage_Destroy(cmsg);
    return NULL;
}

int ceph_decode_cms(CephContext *cct, bufferlist& cms_bl, bufferlist& decoded_bl)
{
    NSSCMSMessage *cmsg = NULL;
    struct decodeOptionsStr decodeOptions = { };
    struct optionsStr options;
    SECItem input;

    memset(&options, 0, sizeof(options));
    memset(&input, 0, sizeof(input));

    input.data = (unsigned char *)cms_bl.c_str();
    input.len = cms_bl.length();

    decodeOptions.content.data = NULL;
    decodeOptions.content.len  = 0;
    decodeOptions.suppressContent = PR_FALSE;
    decodeOptions.headerLevel = -1;
    decodeOptions.keepCerts = PR_FALSE;
    options.certUsage = certUsageEmailSigner;

    options.certHandle = CERT_GetDefaultCertDB();
    if (!options.certHandle) {
	ldout(cct, 0) << "ERROR: No default cert DB" << dendl;
	return -EIO;
    }
    if (cms_verbose) {
	fprintf(stderr, "Got default certdb\n");
    }

    decodeOptions.options = &options;

    int ret = 0;

    cmsg = decode(cct, &input, &decodeOptions, decoded_bl);
    if (!cmsg) {
        ldout(cct, 0) << "ERROR: problem decoding" << dendl;
	ret = -EINVAL;
    }

    if (cmsg)
	NSS_CMSMessage_Destroy(cmsg);

    SECITEM_FreeItem(&decodeOptions.content, PR_FALSE);

    return ret;
}

#endif

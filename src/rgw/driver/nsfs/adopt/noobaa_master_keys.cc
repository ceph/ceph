// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <sys/wait.h>
#include <unistd.h>

#include <openssl/evp.h>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/scope_guard.h"

#include "noobaa_master_keys.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

bool secret_is_plausible(std::string_view secret)
{
  /* A credential has some length.  NooBaa's are 40 characters by
   * default and NooBaa's CLI accepts others, so this is a sanity bound
   * rather than a format. */
  if ((secret.size() < 8) || (secret.size() > 256)) {
    return false;
  }
  for (unsigned char c : secret) {
    /* printable ASCII, which is what a secret is and what keystream
     * almost never is */
    if ((c < 0x20) || (c > 0x7e)) {
      return false;
    }
  }
  return true;
}

namespace {

/* NooBaa's store, whichever form it takes, as JSON text.
 *
 * The executable form is run without a shell:  the path comes out of
 * a configuration file and interpolating it into a command line would
 * be a quoting question nobody needs to have. */
int read_key_store(const DoutPrefixProvider* dpp,
		   const NooBaaSettings& settings,
		   const std::string& config_root,
		   std::string& doc)
{
  const bool from_program = (settings.master_keys_store_type == "executable");

  if (!from_program) {
    /* `file`, NooBaa's default, and the default location is beside
     * config.json rather than anywhere configured. */
    std::string path = settings.master_keys_file_location;
    if (path.empty()) {
      path = config_root + "/master_keys.json";
    } else if (path.find('/') == std::string::npos) {
      path = config_root + "/" + path;
    }
    int ret = read_record_file(dpp, path, doc);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: master keys: " << path << ": "
	<< cpp_strerror(-ret) << dendl;
    }
    return ret;
  }

  const std::string& prog = settings.master_keys_get_executable;
  if (prog.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: NC_MASTER_KEYS_STORE_TYPE is "
      "\"executable\" and NC_MASTER_KEYS_GET_EXECUTABLE is empty" << dendl;
    return -ENOENT;
  }

  int fds[2];
  if (::pipe(fds) != 0) {
    return -errno;
  }
  pid_t pid = ::fork();
  if (pid < 0) {
    int err = -errno;
    ::close(fds[0]);
    ::close(fds[1]);
    return err;
  }
  if (pid == 0) {
    ::close(fds[0]);
    if (::dup2(fds[1], STDOUT_FILENO) < 0) {
      ::_exit(127);
    }
    ::close(fds[1]);
    ::execl(prog.c_str(), prog.c_str(), (char*)nullptr);
    ::_exit(127);
  }
  ::close(fds[1]);

  doc.clear();
  char buf[8192];
  int ret = 0;
  for (;;) {
    ssize_t n = ::read(fds[0], buf, sizeof(buf));
    if (n < 0) {
      ret = -errno;
      break;
    }
    if (n == 0) {
      break;
    }
    /* the same cap the record reader uses:  a key store is small, and
     * a program that never stops writing must not be read forever */
    if ((doc.size() + n) > (1u << 20)) {
      ret = -EFBIG;
      break;
    }
    doc.append(buf, n);
  }
  ::close(fds[0]);

  int status = 0;
  while (::waitpid(pid, &status, 0) < 0) {
    if (errno != EINTR) {
      break;
    }
  }
  if (ret < 0) {
    return ret;
  }
  if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0)) {
    ldpp_dout(dpp, 0) << "ERROR: " << prog << " did not exit cleanly ("
      << status << ")" << dendl;
    return -EIO;
  }
  return 0;
}

int from_base64(const std::string& in, std::vector<unsigned char>& out)
{
  /* OpenSSL's decoder, so there is one base64 in this file and it is
   * not one written here.  Decoded length is bounded by the input. */
  out.assign((in.size() / 4) * 3 + 3, 0);
  int n = EVP_DecodeBlock(out.data(),
			  reinterpret_cast<const unsigned char*>(in.data()),
			  in.size());
  if (n < 0) {
    return -EINVAL;
  }
  /* EVP_DecodeBlock pads to a multiple of three and does not tell us
   * how much of that was padding, so the '=' count has to be
   * subtracted by hand. */
  size_t pad = 0;
  for (auto i = in.rbegin(); (i != in.rend()) && (*i == '='); ++i) {
    ++pad;
  }
  if (static_cast<size_t>(n) < pad) {
    return -EINVAL;
  }
  out.resize(n - pad);
  return 0;
}

} // namespace

int NooBaaMasterKeys::load(const DoutPrefixProvider* dpp,
			   const NooBaaSettings& settings,
			   const std::string& config_root)
{
  std::string doc;
  int ret = read_key_store(dpp, settings, config_root, doc);
  if (ret < 0) {
    return ret;
  }

  JSONParser p;
  if (!p.parse(doc.data(), doc.size())) {
    ldpp_dout(dpp, 0) << "ERROR: the master key store is not JSON" << dendl;
    return -EBADMSG;
  }

  try {
    JSONDecoder::decode_json("active_master_key", active_id, &p);

    JSONObj* all = p.find_obj("master_keys_by_id");
    if (!all) {
      ldpp_dout(dpp, 0) << "ERROR: the master key store has no "
	"master_keys_by_id" << dendl;
      return -ENOKEY;
    }
    for (auto i = all->find_first(); !i.end(); ++i) {
      NooBaaMasterKey k;
      k.id = (*i)->get_name();
      std::string b64_key, b64_iv;
      JSONDecoder::decode_json("id", k.id, *i);
      JSONDecoder::decode_json("cipher_key", b64_key, *i);
      JSONDecoder::decode_json("cipher_iv", b64_iv, *i);
      JSONDecoder::decode_json("encryption_type", k.cipher_type, *i);

      /* Carried and checked, not assumed.  A store naming a cipher
       * this does not implement has to say so:  decrypting it as
       * though it had named NooBaa's usual one would produce garbage that
       * `secret_is_plausible()` would then be the only thing to
       * notice. */
      if (k.cipher_type != "aes-256-gcm") {
	ldpp_dout(dpp, 0) << "ERROR: master key " << k.id << " uses \""
	  << k.cipher_type << "\", which this does not implement" << dendl;
	return -EPROTONOSUPPORT;
      }
      if ((from_base64(b64_key, k.key) < 0) ||
	  (from_base64(b64_iv, k.iv) < 0)) {
	ldpp_dout(dpp, 0) << "ERROR: master key " << k.id
	  << " has a cipher_key or cipher_iv that is not base64" << dendl;
	return -EBADMSG;
      }
      if (k.key.size() != 32) {
	ldpp_dout(dpp, 0) << "ERROR: master key " << k.id << " is "
	  << k.key.size() << " bytes, not 32" << dendl;
	return -EBADMSG;
      }
      /* 16, not GCM's usual 12.  Recorded rather than corrected:  the
       * length is set on the context below, and a key whose IV is some
       * third length would otherwise decrypt to garbage. */
      if (k.iv.empty() || (k.iv.size() > 64)) {
	ldpp_dout(dpp, 0) << "ERROR: master key " << k.id << " has a "
	  << k.iv.size() << "-byte IV" << dendl;
	return -EBADMSG;
      }
      by_id[k.id] = std::move(k);
    }
  } catch (const JSONDecoder::err& e) {
    ldpp_dout(dpp, 0) << "ERROR: the master key store will not decode: "
      << e.what() << dendl;
    return -EBADMSG;
  }

  if (by_id.empty()) {
    return -ENOKEY;
  }
  /* No rotation exists in NC, so the active key is informational
   * here:  a record names the key that encrypted it. */
  if (!active_id.empty() && !have(active_id)) {
    ldpp_dout(dpp, 4) << "nsfs: the active master key " << active_id
      << " is not in the store" << dendl;
  }
  ldpp_dout(dpp, 4) << "nsfs: loaded " << by_id.size()
    << " NooBaa master key(s), active " << active_id << dendl;
  return 0;
}

int NooBaaMasterKeys::decrypt(const DoutPrefixProvider* dpp,
			      const std::string& key_id,
			      const std::string& b64,
			      std::string& out) const
{
  auto i = by_id.find(key_id);
  if (i == by_id.end()) {
    /* §5:  a missing or rotated-out key is a hard failure.  There is
     * nothing to fall back to -- no other key opens this. */
    ldpp_dout(dpp, 0) << "ERROR: no master key " << key_id
      << " in the store" << dendl;
    return -ENOKEY;
  }
  const NooBaaMasterKey& k = i->second;

  std::vector<unsigned char> ct;
  if (from_base64(b64, ct) < 0) {
    return -EINVAL;
  }
  if (ct.empty()) {
    return -EINVAL;
  }

  using ctx_t = std::unique_ptr<EVP_CIPHER_CTX,
				decltype(&::EVP_CIPHER_CTX_free)>;
  ctx_t ctx{EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free};
  if (!ctx) {
    return -ENOMEM;
  }
  if (1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr,
			      nullptr, nullptr)) {
    return -EIO;
  }
  /* The IV length, explicitly.  NooBaa's is 16 bytes and GCM's default
   * is 12, so without this the key and IV are applied to a different
   * construction and every secret decrypts to garbage. */
  if (1 != EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN,
			       static_cast<int>(k.iv.size()), nullptr)) {
    return -EIO;
  }
  if (1 != EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr,
			      k.key.data(), k.iv.data())) {
    return -EIO;
  }

  std::vector<unsigned char> pt(ct.size() + EVP_MAX_BLOCK_LENGTH, 0);
  int len = 0;
  if (1 != EVP_DecryptUpdate(ctx.get(), pt.data(), &len,
			     ct.data(), static_cast<int>(ct.size()))) {
    return -EIO;
  }
  /* No `EVP_DecryptFinal_ex`.  There is no tag to verify:  they never
   * computed one, so asking OpenSSL to check it would fail every
   * secret in every store. */
  out.assign(reinterpret_cast<const char*>(pt.data()), len);

  if (!secret_is_plausible(out)) {
    /* This is the only report a wrong key produces.  The cipher
     * succeeded;  what came out is not a secret. */
    ldpp_dout(dpp, 0) << "ERROR: a secret under master key " << key_id
      << " decrypted to " << len << " bytes that are not a credential."
      << "  The master key is wrong, or this is not the key that "
      << "encrypted it." << dendl;
    out.clear();
    return -EKEYREJECTED;
  }
  return 0;
}

int decrypt_store_secrets(const DoutPrefixProvider* dpp,
			  const NooBaaMasterKeys& keys,
			  NooBaaStoreContents& contents,
			  std::vector<std::string>& failures)
{
  for (auto& id : contents.identities) {
    for (auto& ak : id.access_keys) {
      if (ak.encrypted_secret_key.empty()) {
	/* the anonymous account has an empty key list rather than an
	 * empty key, so this is a record shape we do not expect */
	failures.push_back(id.name + "/" + ak.access_key +
			   ": no encrypted_secret_key");
	continue;
      }
      if (id.master_key_id.empty()) {
	failures.push_back(id.name + ": no master_key_id");
	continue;
      }
      std::string secret;
      int ret = keys.decrypt(dpp, id.master_key_id,
			     ak.encrypted_secret_key, secret);
      if (ret < 0) {
	failures.push_back(id.name + "/" + ak.access_key + ": " +
			   cpp_strerror(-ret));
	continue;
      }
      ak.secret_key = std::move(secret);
    }
  }

  /* Collected, then returned as one error.  Every failure is reported for the same
   * reason I1 reports every unreadable record -- one run should say
   * everything that is wrong -- and the return is still an error, so
   * no caller proceeds past a wrong key by ignoring the list. */
  return failures.empty() ? 0 : -EKEYREJECTED;
}

}}} // namespace rgw::sal::nsfs

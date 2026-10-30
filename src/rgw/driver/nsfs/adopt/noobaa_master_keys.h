// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * NooBaa's master keys, and the secrets they open.
 *
 * I2 of the account import (`ACCOUNT_IMPORT.md` §5, §9).  Read only.
 *
 * An access key's secret is stored as `encrypted_secret_key` and can
 * be decrypted only with the deployment's master key, so an import
 * that cannot open them produces accounts whose credentials do not
 * work.  I1 deliberately left them encrypted;  this opens them.
 *
 * THERE IS NO INTEGRITY CHECK, AND THAT IS THE WHOLE DIFFICULTY.
 * NooBaa's cipher is AES-256-GCM and the authentication tag is never
 * computed or stored -- `nc_master_key_manager.js` calls
 * `cipher.update()` and never `cipher.final()`, in both directions.
 * So a wrong key does not fail:  it returns plaintext-shaped garbage
 * of exactly the right length.  Measured against a real store, a
 * zeroed key turned a 40-character secret into 40 bytes of binary.
 *
 * **So the decryption is checked by what comes out, not by the
 * cipher.**  `secret_is_plausible()` is that check and it is a
 * heuristic, not a guarantee:  a secret is printable ASCII, and 40
 * bytes of keystream being entirely printable has a probability near
 * 1e-17.  Said plainly because the alternative -- treating a
 * successful `EVP_DecryptUpdate` as success -- silently imports
 * credentials that will not authenticate, and nobody learns why.
 *
 * THE IMPORT FAILS ON A SECRET THAT CANNOT BE DECRYPTED.  The secret
 * is not skipped.  §5 makes a missing or rotated-out key a hard failure,
 * and §6.1a makes skipping worse than failing:  an account imported
 * with a garbage secret is a credential that silently stops working.
 *
 * TWO THINGS ABOUT THE CIPHER PARAMETERS.  The IV is **16 bytes**,
 * not GCM's usual 12, so the IV length has to be set explicitly on the
 * context -- a reimplementation that assumed 12, or hand-rolled CTR
 * with `IV||1`, produces wrong plaintext.  And the IV is **fixed per
 * master key**, so every secret under one key shares a keystream.
 * Neither is for nsfs to fix;  both have to be read as NooBaa writes
 * them.
 *
 * Pinned at noobaa-core `08d063f86`.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/dout.h"

#include "noobaa_config_store.h"

namespace rgw { namespace sal { namespace nsfs {

/* One master key, decoded. */
struct NooBaaMasterKey {
  std::string id;
  /* NooBaa's `encryption_type`, carried rather than assumed:  a store
   * naming a cipher this does not implement must say so rather than be
   * decrypted as though it had named the one implemented here */
  std::string cipher_type;
  std::vector<unsigned char> key;
  std::vector<unsigned char> iv;
};

/* Whether a decrypted secret can be a secret.
 *
 * Printable ASCII, no control characters, and a length a credential
 * could have.  **This is the only thing standing between a wrong
 * master key and an import full of credentials that do not work**,
 * because the cipher provides no integrity.  A heuristic, and the
 * probability it passes keystream is about 1e-17 for a 40-byte
 * secret.
 *
 * Exposed, and pure, so both polarities are testable without a
 * cipher. */
bool secret_is_plausible(std::string_view secret);

/* The deployment's master keys.
 *
 * Loaded from whichever store `NC_MASTER_KEYS_STORE_TYPE` names.  No
 * rotation exists in NC, so the set is whatever the store holds and a
 * key a record references and the store lacks is a hard failure. */
class NooBaaMasterKeys {
  std::string active_id;
  std::map<std::string, NooBaaMasterKey> by_id;

public:
  /* Load, honouring the store type.
   *
   *   file        `<location>`, else `<config_root>/master_keys.json`
   *   executable  the program's standard output, which is the same
   *               JSON.  No key file exists in this deployment
   *
   * Returns -ENOENT when the store is not there, -EBADMSG when it is
   * there and will not parse, -ENOKEY when it parses and holds no
   * usable key, and -EPROTONOSUPPORT when a key names a cipher this
   * does not implement. */
  int load(const DoutPrefixProvider* dpp, const NooBaaSettings& settings,
	   const std::string& config_root);

  bool empty() const { return by_id.empty(); }
  size_t size() const { return by_id.size(); }
  const std::string& active() const { return active_id; }
  bool have(const std::string& id) const { return by_id.count(id) > 0; }

  /* Open one secret.
   *
   * `b64` is the record's `encrypted_secret_key` verbatim.  Returns
   * -ENOKEY when `key_id` is not in the store, -EINVAL when the
   * ciphertext is not base64, -EIO when the cipher itself fails, and
   * **-EKEYREJECTED when the plaintext is not plausibly a secret** --
   * which is what a wrong master key looks like, since nothing else
   * will report it. */
  int decrypt(const DoutPrefixProvider* dpp, const std::string& key_id,
	      const std::string& b64, std::string& out) const;
};

/* Open every secret in the store, in place.
 *
 * Fills each `NooBaaAccessKey::secret_key`, and each encrypted
 * `NooBaaConnection::auth`.  Every failure is
 * collected into `failures` rather than stopping at the first, for the
 * same reason I1 collects unreadable records:  one run should say
 * everything that is wrong.  Returns 0 only when every secret opened,
 * so a caller cannot proceed past a wrong key by ignoring a list. */
int decrypt_store_secrets(const DoutPrefixProvider* dpp,
			  const NooBaaMasterKeys& keys,
			  NooBaaStoreContents& contents,
			  std::vector<std::string>& failures);

}}} // namespace rgw::sal::nsfs

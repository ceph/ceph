#pragma once
#define PICOJSON_USE_INT64
#include "picojson/picojson.h"
#include "base.h"
#include <set>
#include <chrono>
#include <unordered_map>
#include <memory>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/bn.h>
#include <openssl/rsa.h>

#include "../rgw_b64.h"

//If openssl version less than 1.1
#if OPENSSL_VERSION_NUMBER < 269484032
#define OPENSSL10
#endif

#ifndef JWT_CLAIM_EXPLICIT
#define JWT_CLAIM_EXPLICIT 1
#endif

namespace jwt {
	using date = std::chrono::system_clock::time_point;

	struct signature_verification_exception : public std::runtime_error {
		signature_verification_exception()
			: std::runtime_error("signature verification failed")
		{}
		explicit signature_verification_exception(const std::string& msg)
			: std::runtime_error(msg)
		{}
		explicit signature_verification_exception(const char* msg)
			: std::runtime_error(msg)
		{}
	};
	struct signature_generation_exception : public std::runtime_error {
		signature_generation_exception()
			: std::runtime_error("signature generation failed")
		{}
		explicit signature_generation_exception(const std::string& msg)
			: std::runtime_error(msg)
		{}
		explicit signature_generation_exception(const char* msg)
			: std::runtime_error(msg)
		{}
	};
	struct rsa_exception : public std::runtime_error {
		explicit rsa_exception(const std::string& msg)
			: std::runtime_error(msg)
		{}
		explicit rsa_exception(const char* msg)
			: std::runtime_error(msg)
		{}
	};
	struct ecdsa_exception : public std::runtime_error {
		explicit ecdsa_exception(const std::string& msg)
			: std::runtime_error(msg)
		{}
		explicit ecdsa_exception(const char* msg)
			: std::runtime_error(msg)
		{}
	};
	struct token_verification_exception : public std::runtime_error {
		token_verification_exception()
			: std::runtime_error("token verification failed")
		{}
		explicit token_verification_exception(const std::string& msg)
			: std::runtime_error("token verification failed: " + msg)
		{}
	};

	namespace helper {
		inline
		std::string extract_pubkey_from_cert(const std::string& certstr, const std::string& pw = "") {
			// TODO: Cannot find the exact version this change happended
#if OPENSSL_VERSION_NUMBER <= 0x1000114fL
			std::unique_ptr<BIO, decltype(&BIO_free_all)> certbio(BIO_new_mem_buf(const_cast<char*>(certstr.data()), certstr.size()), BIO_free_all);
#else
			std::unique_ptr<BIO, decltype(&BIO_free_all)> certbio(BIO_new_mem_buf(certstr.data(), certstr.size()), BIO_free_all);
#endif
			std::unique_ptr<BIO, decltype(&BIO_free_all)> keybio(BIO_new(BIO_s_mem()), BIO_free_all);

			std::unique_ptr<X509, decltype(&X509_free)> cert(PEM_read_bio_X509(certbio.get(), nullptr, nullptr, const_cast<char*>(pw.c_str())), X509_free);
			if (!cert) throw rsa_exception("Error loading cert into memory");
			std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(X509_get_pubkey(cert.get()), EVP_PKEY_free);
			if(!key) throw rsa_exception("Error getting public key from certificate");
			if(!PEM_write_bio_PUBKEY(keybio.get(), key.get())) throw rsa_exception("Error writing public key data in PEM format");
			char* ptr = nullptr;
			auto len = BIO_get_mem_data(keybio.get(), &ptr);
			if(len <= 0 || ptr == nullptr) throw rsa_exception("Failed to convert pubkey to pem");
			std::string res(ptr, len);
			return res;
		}

		inline
		std::shared_ptr<EVP_PKEY> load_public_key_from_string(const std::string& key, const std::string& password = "") {
			std::unique_ptr<BIO, decltype(&BIO_free_all)> pubkey_bio(BIO_new(BIO_s_mem()), BIO_free_all);
			if(key.substr(0, 27) == "-----BEGIN CERTIFICATE-----") {
				auto epkey = helper::extract_pubkey_from_cert(key, password);
				if ((size_t)BIO_write(pubkey_bio.get(), epkey.data(), epkey.size()) != epkey.size())
					throw rsa_exception("failed to load public key: bio_write failed");
			} else {
				if ((size_t)BIO_write(pubkey_bio.get(), key.data(), key.size()) != key.size())
					throw rsa_exception("failed to load public key: bio_write failed");
			}
			
			std::shared_ptr<EVP_PKEY> pkey(PEM_read_bio_PUBKEY(pubkey_bio.get(), nullptr, nullptr, (void*)password.c_str()), EVP_PKEY_free);
			if (!pkey)
				throw rsa_exception("failed to load public key: PEM_read_bio_PUBKEY failed:" + std::string(ERR_error_string(ERR_get_error(), NULL)));
			return pkey;
		}

		inline
		std::shared_ptr<EVP_PKEY> load_private_key_from_string(const std::string& key, const std::string& password = "") {
			std::unique_ptr<BIO, decltype(&BIO_free_all)> privkey_bio(BIO_new(BIO_s_mem()), BIO_free_all);
			if ((size_t)BIO_write(privkey_bio.get(), key.data(), key.size()) != key.size())
				throw rsa_exception("failed to load private key: bio_write failed");
			std::shared_ptr<EVP_PKEY> pkey(PEM_read_bio_PrivateKey(privkey_bio.get(), nullptr, nullptr, const_cast<char*>(password.c_str())), EVP_PKEY_free);
			if (!pkey)
				throw rsa_exception("failed to load private key: PEM_read_bio_PrivateKey failed");
			return pkey;
		}
	}

	namespace algorithm {
		/**
		 * "none" algorithm.
		 * 
		 * Returns and empty signature and checks if the given signature is empty.
		 */
		struct none {
			/// Return an empty string
			std::string sign(const std::string&) const {
				return "";
			}
			/// Check if the given signature is empty. JWT's with "none" algorithm should not contain a signature.
			void verify(const std::string&, const std::string& signature) const {
				if (!signature.empty())
					throw signature_verification_exception();
			}
			/// Get algorithm name
			std::string name() const {
				return "none";
			}
		};
		/**
		 * Base class for HMAC family of algorithms
		 */
		struct hmacsha {
			/**
			 * Construct new hmac algorithm
			 * \param key Key to use for HMAC
			 * \param md Pointer to hash function
			 * \param name Name of the algorithm
			 */
			hmacsha(std::string key, const EVP_MD*(*md)(), const std::string& name)
				: secret(std::move(key)), md(md), alg_name(name)
			{}
			/**
			 * Sign jwt data
			 * \param data The data to sign
			 * \return HMAC signature for the given data
			 * \throws signature_generation_exception
			 */
			std::string sign(const std::string& data) const {
				std::string res;
				res.resize(EVP_MAX_MD_SIZE);
				unsigned int len = res.size();
				if (HMAC(md(), secret.data(), secret.size(), (const unsigned char*)data.data(), data.size(), (unsigned char*)res.data(), &len) == nullptr)
					throw signature_generation_exception();
				res.resize(len);
				return res;
			}
			/**
			 * Check if signature is valid
			 * \param data The data to check signature against
			 * \param signature Signature provided by the jwt
			 * \throws signature_verification_exception If the provided signature does not match
			 */
			void verify(const std::string& data, const std::string& signature) const {
				try {
					auto res = sign(data);
					bool matched = true;
					for (size_t i = 0; i < std::min<size_t>(res.size(), signature.size()); i++)
						if (res[i] != signature[i])
							matched = false;
					if (res.size() != signature.size())
						matched = false;
					if (!matched)
						throw signature_verification_exception();
				}
				catch (const signature_generation_exception&) {
					throw signature_verification_exception();
				}
			}
			/**
			 * Returns the algorithm name provided to the constructor
			 * \return Algorithmname
			 */
			std::string name() const {
				return alg_name;
			}
		private:
			/// HMAC secrect
			const std::string secret;
			/// HMAC hash generator
			const EVP_MD*(*md)();
			/// Algorithmname
			const std::string alg_name;
		};
		/**
		 * Base class for RSA family of algorithms
		 */
		struct rsa {
			/**
			 * Construct new rsa algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 * \param md Pointer to hash function
			 * \param name Name of the algorithm
			 */
			rsa(const std::string& public_key, const std::string& private_key, const std::string& public_key_password, const std::string& private_key_password, const EVP_MD*(*md)(), const std::string& name)
				: md(md), alg_name(name)
			{
				if (!private_key.empty()) {
					pkey = helper::load_private_key_from_string(private_key, private_key_password);
				} else if(!public_key.empty()) {
					pkey = helper::load_public_key_from_string(public_key, public_key_password);
				} else
					throw rsa_exception("at least one of public or private key need to be present");
			}
			rsa(const EVP_MD*(*md)(), const std::string& name) : md(md), alg_name(name)
			{}

			void setModulusExponentCalcPublicKey(const string& n, const string& exponent)
			{
				this->modulus = modulus;
				this->exponent = exponent;
				calculatePublicKey();
			}

			/**
			 * Sign jwt data
			 * \param data The data to sign
			 * \return RSA signature for the given data
			 * \throws signature_generation_exception
			 */
			std::string sign(const std::string& data) const {
#ifdef OPENSSL10
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_destroy)> ctx(EVP_MD_CTX_create(), EVP_MD_CTX_destroy);
#else
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_create(), EVP_MD_CTX_free);
#endif
				if (!ctx)
					throw signature_generation_exception("failed to create signature: could not create context");
				if (!EVP_SignInit(ctx.get(), md()))
					throw signature_generation_exception("failed to create signature: SignInit failed");

				std::string res;
				res.resize(EVP_PKEY_size(pkey.get()));
				unsigned int len = 0;

				if (!EVP_SignUpdate(ctx.get(), data.data(), data.size()))
					throw signature_generation_exception();
				if (!EVP_SignFinal(ctx.get(), (unsigned char*)res.data(), &len, pkey.get()))
					throw signature_generation_exception();

				res.resize(len);
				return res;
			}

			/**
			 * Check if signature is valid
			 * \param data The data to check signature against
			 * \param signature Signature provided by the jwt
			 * \throws signature_verification_exception If the provided signature does not match
			 */
			void verify(const std::string& data, const std::string& signature) const {
#ifdef OPENSSL10
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_destroy)> ctx(EVP_MD_CTX_create(), EVP_MD_CTX_destroy);
#else
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_create(), EVP_MD_CTX_free);
#endif
				if (!ctx)
					throw signature_verification_exception("failed to verify signature: could not create context");
				if (!EVP_VerifyInit(ctx.get(), md()))
					throw signature_verification_exception("failed to verify signature: VerifyInit failed");
				if (!EVP_VerifyUpdate(ctx.get(), data.data(), data.size()))
					throw signature_verification_exception("failed to verify signature: VerifyUpdate failed");

				auto res = EVP_VerifyFinal(ctx.get(), (const unsigned char*)signature.data(), signature.size(), pkey.get());
				if (res != 1)
					throw signature_verification_exception("evp verify final failed: " + std::to_string(res) + " " + ERR_error_string(ERR_get_error(), NULL));
			}
			/**
			 * Returns the algorithm name provided to the constructor
			 * \return Algorithmname
			 */
			std::string name() const {
				return alg_name;
			}
		private:
			/// OpenSSL structure containing converted keys
			std::shared_ptr<EVP_PKEY> pkey;
			/// Hash generator
			const EVP_MD*(*md)();
			// Modulus
			std::string modulus;
			// Exponent
			std::string exponent;
			/// Algorithmname
			const std::string alg_name;

			void calculatePublicKey()
			{
				string n_str = base64_decode_url(modulus);
				string e_str = base64_decode_url(exponent);
				unsigned char* u_n = (unsigned char *)n_str.c_str();
				unsigned char* u_e = (unsigned char *)e_str.c_str();
				BIGNUM *n = BN_bin2bn(u_n, n_str.size(), NULL);
				BIGNUM *e = BN_bin2bn(u_e, e_str.size(), NULL);

				if (e && n) {
					EVP_PKEY* pRsaKey = EVP_PKEY_new();
					RSA* rsa = RSA_new();
					RSA_set0_key(rsa, n, e, nullptr);
					EVP_PKEY_assign_RSA(pRsaKey, rsa);
					shared_ptr<EVP_PKEY> p(pRsaKey, EVP_PKEY_free);
					pkey = p;
				} else {
					if (n) BN_free(n);
					if (e) BN_free(e);
					throw rsa_exception("Invalid encoding for modulus or exponent\n");
				}
			}

			string base64_decode_url(const string& str) const {
				string s = "====";
				string padded_str = str.length() % 4 == 0 ? str : str + s.substr(0, str.length() % 4);
				std::replace(padded_str.begin(), padded_str.end(), '_', '/');
				std::replace(padded_str.begin(), padded_str.end(), '-', '+');
				return rgw::from_base64(padded_str);
			}
		};
		/**
		 * Base class for ECDSA family of algorithms
		 */
		struct ecdsa {
			/**
			 * Construct new ecdsa algorithm
			 * \param public_key ECDSA public key in PEM format
			 * \param private_key ECDSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 * \param md Pointer to hash function
			 * \param name Name of the algorithm
			 */
			ecdsa(const std::string& public_key, const std::string& private_key, const std::string& public_key_password, const std::string& private_key_password, const EVP_MD*(*md)(), const std::string& name, size_t siglen)
				: md(md), alg_name(name), signature_length(siglen)
			{
				if (!public_key.empty()) {
					std::unique_ptr<BIO, decltype(&BIO_free_all)> pubkey_bio(BIO_new(BIO_s_mem()), BIO_free_all);
					if(public_key.substr(0, 27) == "-----BEGIN CERTIFICATE-----") {
						auto epkey = helper::extract_pubkey_from_cert(public_key, public_key_password);
						if ((size_t)BIO_write(pubkey_bio.get(), epkey.data(), epkey.size()) != epkey.size())
							throw ecdsa_exception("failed to load public key: bio_write failed");
					} else  {
						if ((size_t)BIO_write(pubkey_bio.get(), public_key.data(), public_key.size()) != public_key.size())
							throw ecdsa_exception("failed to load public key: bio_write failed");
					}

					pkey.reset(PEM_read_bio_EC_PUBKEY(pubkey_bio.get(), nullptr, nullptr, (void*)public_key_password.c_str()), EC_KEY_free);
					if (!pkey)
						throw ecdsa_exception("failed to load public key: PEM_read_bio_EC_PUBKEY failed:" + std::string(ERR_error_string(ERR_get_error(), NULL)));
					size_t keysize = EC_GROUP_get_degree(EC_KEY_get0_group(pkey.get()));
					if(keysize != signature_length*4 && (signature_length != 132 || keysize != 521))
						throw ecdsa_exception("invalid key size");
				}

				if (!private_key.empty()) {
					std::unique_ptr<BIO, decltype(&BIO_free_all)> privkey_bio(BIO_new(BIO_s_mem()), BIO_free_all);
					if ((size_t)BIO_write(privkey_bio.get(), private_key.data(), private_key.size()) != private_key.size())
						throw rsa_exception("failed to load private key: bio_write failed");
					pkey.reset(PEM_read_bio_ECPrivateKey(privkey_bio.get(), nullptr, nullptr, const_cast<char*>(private_key_password.c_str())), EC_KEY_free);
					if (!pkey)
						throw rsa_exception("failed to load private key: PEM_read_bio_ECPrivateKey failed");
					size_t keysize = EC_GROUP_get_degree(EC_KEY_get0_group(pkey.get()));
					if(keysize != signature_length*4 && (signature_length != 132 || keysize != 521))
						throw ecdsa_exception("invalid key size");
				}
				if(!pkey)
					throw rsa_exception("at least one of public or private key need to be present");

				if(EC_KEY_check_key(pkey.get()) == 0)
					throw ecdsa_exception("failed to load key: key is invalid");
			}
			/**
			 * Sign jwt data
			 * \param data The data to sign
			 * \return ECDSA signature for the given data
			 * \throws signature_generation_exception
			 */
			std::string sign(const std::string& data) const {
				const std::string hash = generate_hash(data);

				std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)>
					sig(ECDSA_do_sign((const unsigned char*)hash.data(), hash.size(), pkey.get()), ECDSA_SIG_free);
				if(!sig)
					throw signature_generation_exception();
#ifdef OPENSSL10

				auto rr = bn2raw(sig->r);
				auto rs = bn2raw(sig->s);
#else
				const BIGNUM *r;
				const BIGNUM *s;
				ECDSA_SIG_get0(sig.get(), &r, &s);
				auto rr = bn2raw(r);
				auto rs = bn2raw(s);
#endif
				if(rr.size() > signature_length/2 || rs.size() > signature_length/2)
					throw std::logic_error("bignum size exceeded expected length");
				while(rr.size() != signature_length/2) rr = '\0' + rr;
				while(rs.size() != signature_length/2) rs = '\0' + rs;
				return rr + rs;
			}

			/**
			 * Check if signature is valid
			 * \param data The data to check signature against
			 * \param signature Signature provided by the jwt
			 * \throws signature_verification_exception If the provided signature does not match
			 */
			void verify(const std::string& data, const std::string& signature) const {
				const std::string hash = generate_hash(data);
				auto r = raw2bn(signature.substr(0, signature.size() / 2));
				auto s = raw2bn(signature.substr(signature.size() / 2));

#ifdef OPENSSL10
				ECDSA_SIG sig;
				sig.r = r.get();
				sig.s = s.get();

				if(ECDSA_do_verify((const unsigned char*)hash.data(), hash.size(), &sig, pkey.get()) != 1)
					throw signature_verification_exception("Invalid signature");
#else
				std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)> sig(ECDSA_SIG_new(), ECDSA_SIG_free);

				ECDSA_SIG_set0(sig.get(), r.release(), s.release());

				if(ECDSA_do_verify((const unsigned char*)hash.data(), hash.size(), sig.get(), pkey.get()) != 1)
					throw signature_verification_exception("Invalid signature");
#endif
			}
			/**
			 * Returns the algorithm name provided to the constructor
			 * \return Algorithmname
			 */
			std::string name() const {
				return alg_name;
			}
		private:
			/**
			 * Convert a OpenSSL BIGNUM to a std::string
			 * \param bn BIGNUM to convert
			 * \return bignum as string
			 */
#ifdef OPENSSL10
			static std::string bn2raw(BIGNUM* bn)
#else
			static std::string bn2raw(const BIGNUM* bn)
#endif
			{
				std::string res;
				res.resize(BN_num_bytes(bn));
				BN_bn2bin(bn, (unsigned char*)res.data());
				return res;
			}
			/**
			 * Convert an std::string to a OpenSSL BIGNUM
			 * \param raw String to convert
			 * \return BIGNUM representation
			 */
			static std::unique_ptr<BIGNUM, decltype(&BN_free)> raw2bn(const std::string& raw) {
				return std::unique_ptr<BIGNUM, decltype(&BN_free)>(BN_bin2bn((const unsigned char*)raw.data(), raw.size(), nullptr), BN_free);
			}

			/**
			 * Hash the provided data using the hash function specified in constructor
			 * \param data Data to hash
			 * \return Hash of data
			 */
			std::string generate_hash(const std::string& data) const {
#ifdef OPENSSL10
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_destroy)> ctx(EVP_MD_CTX_create(), &EVP_MD_CTX_destroy);
#else
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
#endif
				if(EVP_DigestInit(ctx.get(), md()) == 0)
					throw signature_generation_exception("EVP_DigestInit failed");
				if(EVP_DigestUpdate(ctx.get(), data.data(), data.size()) == 0)
					throw signature_generation_exception("EVP_DigestUpdate failed");
				unsigned int len = 0;
				std::string res;
				res.resize(EVP_MD_CTX_size(ctx.get()));
				if(EVP_DigestFinal(ctx.get(), (unsigned char*)res.data(), &len) == 0)
					throw signature_generation_exception("EVP_DigestFinal failed");
				res.resize(len);
				return res;
			}

			/// OpenSSL struct containing keys
			std::shared_ptr<EC_KEY> pkey;
			/// Hash generator function
			const EVP_MD*(*md)();
			/// Algorithmname
			const std::string alg_name;
			/// Length of the resulting signature
			const size_t signature_length;
		};

		/**
		 * Base class for PSS-RSA family of algorithms
		 */
		struct pss {
			/**
			 * Construct new pss algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 * \param md Pointer to hash function
			 * \param name Name of the algorithm
			 */
			pss(const std::string& public_key, const std::string& private_key, const std::string& public_key_password, const std::string& private_key_password, const EVP_MD*(*md)(), const std::string& name)
				: md(md), alg_name(name)
			{
				if (!private_key.empty()) {
					pkey = helper::load_private_key_from_string(private_key, private_key_password);
				} else if(!public_key.empty()) {
					pkey = helper::load_public_key_from_string(public_key, public_key_password);
				} else
					throw rsa_exception("at least one of public or private key need to be present");
			}
			/**
			 * Sign jwt data
			 * \param data The data to sign
			 * \return ECDSA signature for the given data
			 * \throws signature_generation_exception
			 */
			std::string sign(const std::string& data) const {
				auto hash = this->generate_hash(data);

				std::unique_ptr<RSA, decltype(&RSA_free)> key(EVP_PKEY_get1_RSA(pkey.get()), RSA_free);
				const int size = RSA_size(key.get());

				std::string padded(size, 0x00);
				if (!RSA_padding_add_PKCS1_PSS_mgf1(key.get(), (unsigned char*)padded.data(), (const unsigned char*)hash.data(), md(), md(), -1))  
					throw signature_generation_exception("failed to create signature: RSA_padding_add_PKCS1_PSS_mgf1 failed");

				std::string res(size, 0x00);
				if (RSA_private_encrypt(size, (const unsigned char*)padded.data(), (unsigned char*)res.data(), key.get(), RSA_NO_PADDING) < 0)
					throw signature_generation_exception("failed to create signature: RSA_private_encrypt failed");
				return res;
			}
			/**
			 * Check if signature is valid
			 * \param data The data to check signature against
			 * \param signature Signature provided by the jwt
			 * \throws signature_verification_exception If the provided signature does not match
			 */
			void verify(const std::string& data, const std::string& signature) const {
				auto hash = this->generate_hash(data);

				std::unique_ptr<RSA, decltype(&RSA_free)> key(EVP_PKEY_get1_RSA(pkey.get()), RSA_free);
				const int size = RSA_size(key.get());
				
				std::string sig(size, 0x00);
				if(!RSA_public_decrypt(signature.size(), (const unsigned char*)signature.data(), (unsigned char*)sig.data(), key.get(), RSA_NO_PADDING))
					throw signature_verification_exception("Invalid signature");
				
				if(!RSA_verify_PKCS1_PSS_mgf1(key.get(), (const unsigned char*)hash.data(), md(), md(), (const unsigned char*)sig.data(), -1))
					throw signature_verification_exception("Invalid signature");
			}
			/**
			 * Returns the algorithm name provided to the constructor
			 * \return Algorithmname
			 */
			std::string name() const {
				return alg_name;
			}
		private:
			/**
			 * Hash the provided data using the hash function specified in constructor
			 * \param data Data to hash
			 * \return Hash of data
			 */
			std::string generate_hash(const std::string& data) const {
#ifdef OPENSSL10
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_destroy)> ctx(EVP_MD_CTX_create(), &EVP_MD_CTX_destroy);
#else
				std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
#endif
				if(EVP_DigestInit(ctx.get(), md()) == 0)
					throw signature_generation_exception("EVP_DigestInit failed");
				if(EVP_DigestUpdate(ctx.get(), data.data(), data.size()) == 0)
					throw signature_generation_exception("EVP_DigestUpdate failed");
				unsigned int len = 0;
				std::string res;
				res.resize(EVP_MD_CTX_size(ctx.get()));
				if(EVP_DigestFinal(ctx.get(), (unsigned char*)res.data(), &len) == 0)
					throw signature_generation_exception("EVP_DigestFinal failed");
				res.resize(len);
				return res;
			}
			
			/// OpenSSL structure containing keys
			std::shared_ptr<EVP_PKEY> pkey;
			/// Hash generator function
			const EVP_MD*(*md)();
			/// Algorithmname
			const std::string alg_name;
		};

		/**
		 * HS256 algorithm
		 */
		struct hs256 : public hmacsha {
			/**
			 * Construct new instance of algorithm
			 * \param key HMAC signing key
			 */
			explicit hs256(std::string key)
				: hmacsha(std::move(key), EVP_sha256, "HS256")
			{}
		};
		/**
		 * HS384 algorithm
		 */
		struct hs384 : public hmacsha {
			/**
			 * Construct new instance of algorithm
			 * \param key HMAC signing key
			 */
			explicit hs384(std::string key)
				: hmacsha(std::move(key), EVP_sha384, "HS384")
			{}
		};
		/**
		 * HS512 algorithm
		 */
		struct hs512 : public hmacsha {
			/**
			 * Construct new instance of algorithm
			 * \param key HMAC signing key
			 */
			explicit hs512(std::string key)
				: hmacsha(std::move(key), EVP_sha512, "HS512")
			{}
		};
		/**
		 * RS256 algorithm
		 */
		struct rs256 : public rsa {
			/**
			 * Construct new instance of algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit rs256(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: rsa(public_key, private_key, public_key_password, private_key_password, EVP_sha256, "RS256")
			{}

			rs256() : rsa(EVP_sha256, "RS256")
			{}

			rs256& setModulusAndExponent(const string& modulus, const string& exponent)
			{
				rsa::setModulusExponentCalcPublicKey(modulus, exponent);
				return *this;
			}
		};
		/**
		 * RS384 algorithm
		 */
		struct rs384 : public rsa {
			/**
			 * Construct new instance of algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit rs384(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: rsa(public_key, private_key, public_key_password, private_key_password, EVP_sha384, "RS384")
			{}

			rs384() : rsa(EVP_sha384, "RS384")
			{}

			rs384& setModulusAndExponent(const string& modulus, const string& exponent)
			{
				rsa::setModulusExponentCalcPublicKey(modulus, exponent);
				return *this;
			}
		};
		/**
		 * RS512 algorithm
		 */
		struct rs512 : public rsa {
			/**
			 * Construct new instance of algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit rs512(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: rsa(public_key, private_key, public_key_password, private_key_password, EVP_sha512, "RS512")
			{}

			rs512() : rsa(EVP_sha512, "RS512")
			{}

			rs512& setModulusAndExponent(const string& modulus, const string& exponent)
			{
				rsa::setModulusExponentCalcPublicKey(modulus, exponent);
				return *this;
			}
		};
		/**
		 * ES256 algorithm
		 */
		struct es256 : public ecdsa {
			/**
			 * Construct new instance of algorithm
			 * \param public_key ECDSA public key in PEM format
			 * \param private_key ECDSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit es256(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: ecdsa(public_key, private_key, public_key_password, private_key_password, EVP_sha256, "ES256", 64)
			{}
		};
		/**
		 * ES384 algorithm
		 */
		struct es384 : public ecdsa {
			/**
			 * Construct new instance of algorithm
			 * \param public_key ECDSA public key in PEM format
			 * \param private_key ECDSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit es384(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: ecdsa(public_key, private_key, public_key_password, private_key_password, EVP_sha384, "ES384", 96)
			{}
		};
		/**
		 * ES512 algorithm
		 */
		struct es512 : public ecdsa {
			/**
			 * Construct new instance of algorithm
			 * \param public_key ECDSA public key in PEM format
			 * \param private_key ECDSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit es512(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: ecdsa(public_key, private_key, public_key_password, private_key_password, EVP_sha512, "ES512", 132)
			{}
		};

		/**
		 * PS256 algorithm
		 */
		struct ps256 : public pss {
			/**
			 * Construct new instance of algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit ps256(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: pss(public_key, private_key, public_key_password, private_key_password, EVP_sha256, "PS256")
			{}
		};
		/**
		 * PS384 algorithm
		 */
		struct ps384 : public pss {
			/**
			 * Construct new instance of algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit ps384(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: pss(public_key, private_key, public_key_password, private_key_password, EVP_sha384, "PS384")
			{}
		};
		/**
		 * PS512 algorithm
		 */
		struct ps512 : public pss {
			/**
			 * Construct new instance of algorithm
			 * \param public_key RSA public key in PEM format
			 * \param private_key RSA private key or empty string if not available. If empty, signing will always fail.
			 * \param public_key_password Password to decrypt public key pem.
			 * \param privat_key_password Password to decrypt private key pem.
			 */
			explicit ps512(const std::string& public_key, const std::string& private_key = "", const std::string& public_key_password = "", const std::string& private_key_password = "")
				: pss(public_key, private_key, public_key_password, private_key_password, EVP_sha512, "PS512")
			{}
		};
	}

	/**
	 * Convenience wrapper for JSON value
	 */
	class claim {
		picojson::value val;
	public:
		enum class type {
			null,
			boolean,
			number,
			string,
			array,
			object,
			int64
		};

		claim()
			: val()
		{}
#if JWT_CLAIM_EXPLICIT
		explicit claim(std::string s)
			: val(std::move(s))
		{}
		explicit claim(const date& s)
			: val(int64_t(std::chrono::system_clock::to_time_t(s)))
		{}
		explicit claim(const std::set<std::string>& s)
			: val(picojson::array(s.cbegin(), s.cend()))
		{}
		explicit claim(const picojson::value& val)
			: val(val)
		{}
#else
		claim(std::string s)
			: val(std::move(s))
		{}
		claim(const date& s)
			: val(int64_t(std::chrono::system_clock::to_time_t(s)))
		{}
		claim(const std::set<std::string>& s)
			: val(picojson::array(s.cbegin(), s.cend()))
		{}
		claim(const picojson::value& val)
			: val(val)
		{}
#endif

		template<typename Iterator>
		claim(Iterator start, Iterator end)
			: val(picojson::array())
		{
			auto& arr = val.get<picojson::array>();
			for(; start != end; start++) {
				arr.push_back(picojson::value(*start));
			}
		}

		/**
		 * Get wrapped json object
		 * \return Wrapped json object
		 */
		picojson::value to_json() const {
			return val;
		}

		/**
		 * Get type of contained object
		 * \return Type
		 * \throws std::logic_error An internal error occured
		 */
		type get_type() const {
			if (val.is<picojson::null>()) return type::null;
			else if (val.is<bool>()) return type::boolean;
			else if (val.is<int64_t>()) return type::int64;
			else if (val.is<double>()) return type::number;
			else if (val.is<std::string>()) return type::string;
			else if (val.is<picojson::array>()) return type::array;
			else if (val.is<picojson::object>()) return type::object;
			else throw std::logic_error("internal error");
		}

		/**
		 * Get the contained object as a string
		 * \return content as string
		 * \throws std::bad_cast Content was not a string
		 */
		const std::string& as_string() const {
			if (!val.is<std::string>())
				throw std::bad_cast();
			return val.get<std::string>();
		}
		/**
		 * Get the contained object as a date
		 * \return content as date
		 * \throws std::bad_cast Content was not a date
		 */
		date as_date() const {
			return std::chrono::system_clock::from_time_t(as_int());
		}
		/**
		 * Get the contained object as an array
		 * \return content as array
		 * \throws std::bad_cast Content was not an array
		 */
		const picojson::array& as_array() const {
			if (!val.is<picojson::array>())
				throw std::bad_cast();
			return val.get<picojson::array>();
		}
		/**
		 * Get the contained object as a set of strings
		 * \return content as set of strings
		 * \throws std::bad_cast Content was not a set
		 */
		const std::set<std::string> as_set() const {
			std::set<std::string> res;
			for(auto& e : as_array()) {
				if(!e.is<std::string>())
					throw std::bad_cast();
				res.insert(e.get<std::string>());
			}
			return res;
		}
		/**
		 * Get the contained object as an integer
		 * \return content as int
		 * \throws std::bad_cast Content was not an int
		 */
		int64_t as_int() const {
			if (!val.is<int64_t>())
				throw std::bad_cast();
			return val.get<int64_t>();
		}
		/**
		 * Get the contained object as a bool
		 * \return content as bool
		 * \throws std::bad_cast Content was not a bool
		 */
		bool as_bool() const {
			if (!val.is<bool>())
				throw std::bad_cast();
			return val.get<bool>();
		}
		/**
		 * Get the contained object as a number
		 * \return content as double
		 * \throws std::bad_cast Content was not a number
		 */
		double as_number() const {
			if (!val.is<double>())
				throw std::bad_cast();
			return val.get<double>();
		}
	};

	/**
	 * Base class that represents a token payload.
	 * Contains Convenience accessors for common claims.
	 */
	class payload {
	protected:
		std::unordered_map<std::string, claim> payload_claims;
	public:
		/**
		 * Check if issuer is present ("iss")
		 * \return true if present, false otherwise
		 */
		bool has_issuer() const noexcept { return has_payload_claim("iss"); }
		/**
		 * Check if subject is present ("sub")
		 * \return true if present, false otherwise
		 */
		bool has_subject() const noexcept { return has_payload_claim("sub"); }
		/**
		 * Check if audience is present ("aud")
		 * \return true if present, false otherwise
		 */
		bool has_audience() const noexcept { return has_payload_claim("aud"); }
		/**
		 * Check if expires is present ("exp")
		 * \return true if present, false otherwise
		 */
		bool has_expires_at() const noexcept { return has_payload_claim("exp"); }
		/**
		 * Check if not before is present ("nbf")
		 * \return true if present, false otherwise
		 */
		bool has_not_before() const noexcept { return has_payload_claim("nbf"); }
		/**
		 * Check if issued at is present ("iat")
		 * \return true if present, false otherwise
		 */
		bool has_issued_at() const noexcept { return has_payload_claim("iat"); }
		/**
		 * Check if token id is present ("jti")
		 * \return true if present, false otherwise
		 */
		bool has_id() const noexcept { return has_payload_claim("jti"); }
		/**
		 * Get issuer claim
		 * \return issuer as string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_issuer() const { return get_payload_claim("iss").as_string(); }
		/**
		 * Get subject claim
		 * \return subject as string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_subject() const { return get_payload_claim("sub").as_string(); }
		/**
		 * Get audience claim
		 * \return audience as a set of strings
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a set (Should not happen in a valid token)
		 */
		std::set<std::string> get_audience() const { 
			auto aud = get_payload_claim("aud");
			if(aud.get_type() == jwt::claim::type::string) return { aud.as_string()};
			else return aud.as_set();
		}
		/**
		 * Get expires claim
		 * \return expires as a date in utc
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a date (Should not happen in a valid token)
		 */
		const date get_expires_at() const { return get_payload_claim("exp").as_date(); }
		/**
		 * Get not valid before claim
		 * \return nbf date in utc
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a date (Should not happen in a valid token)
		 */
		const date get_not_before() const { return get_payload_claim("nbf").as_date(); }
		/**
		 * Get issued at claim
		 * \return issued at as date in utc
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a date (Should not happen in a valid token)
		 */
		const date get_issued_at() const { return get_payload_claim("iat").as_date(); }
		/**
		 * Get id claim
		 * \return id as string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_id() const { return get_payload_claim("jti").as_string(); }
		/**
		 * Check if a payload claim is present
		 * \return true if claim was present, false otherwise
		 */
		bool has_payload_claim(const std::string& name) const noexcept { return payload_claims.count(name) != 0; }
		/**
		 * Get payload claim
		 * \return Requested claim
		 * \throws std::runtime_error If claim was not present
		 */
		const claim& get_payload_claim(const std::string& name) const {
			if (!has_payload_claim(name))
				throw std::runtime_error("claim not found");
			return payload_claims.at(name);
		}
		/**
		 * Get all payload claims
		 * \return map of claims
		 */
		std::unordered_map<std::string, claim> get_payload_claims() const { return payload_claims; }
	};

	/**
	 * Base class that represents a token header.
	 * Contains Convenience accessors for common claims.
	 */
	class header {
	protected:
		std::unordered_map<std::string, claim> header_claims;
	public:
		/**
		 * Check if algortihm is present ("alg")
		 * \return true if present, false otherwise
		 */
		bool has_algorithm() const noexcept { return has_header_claim("alg"); }
		/**
		 * Check if type is present ("typ")
		 * \return true if present, false otherwise
		 */
		bool has_type() const noexcept { return has_header_claim("typ"); }
		/**
		 * Check if content type is present ("cty")
		 * \return true if present, false otherwise
		 */
		bool has_content_type() const noexcept { return has_header_claim("cty"); }
		/**
		 * Check if key id is present ("kid")
		 * \return true if present, false otherwise
		 */
		bool has_key_id() const noexcept { return has_header_claim("kid"); }
		/**
		 * Get algorithm claim
		 * \return algorithm as string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_algorithm() const { return get_header_claim("alg").as_string(); }
		/**
		 * Get type claim
		 * \return type as a string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_type() const { return get_header_claim("typ").as_string(); }
		/**
		 * Get content type claim
		 * \return content type as string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_content_type() const { return get_header_claim("cty").as_string(); }
		/**
		 * Get key id claim
		 * \return key id as string
		 * \throws std::runtime_error If claim was not present
		 * \throws std::bad_cast Claim was present but not a string (Should not happen in a valid token)
		 */
		const std::string& get_key_id() const { return get_header_claim("kid").as_string(); }
		/**
		 * Check if a header claim is present
		 * \return true if claim was present, false otherwise
		 */
		bool has_header_claim(const std::string& name) const noexcept { return header_claims.count(name) != 0; }
		/**
		 * Get header claim
		 * \return Requested claim
		 * \throws std::runtime_error If claim was not present
		 */
		const claim& get_header_claim(const std::string& name) const {
			if (!has_header_claim(name))
				throw std::runtime_error("claim not found");
			return header_claims.at(name);
		}
		/**
		 * Get all header claims
		 * \return map of claims
		 */
		std::unordered_map<std::string, claim> get_header_claims() const { return header_claims; }
	};

	/**
	 * Class containing all information about a decoded token
	 */
	class decoded_jwt : public header, public payload {
	protected:
		/// Unmodifed token, as passed to constructor
		const std::string token;
		/// Header part decoded from base64
		std::string header;
		/// Unmodified header part in base64
		std::string header_base64;
		/// Payload part decoded from base64
		std::string payload;
		/// Unmodified payload part in base64
		std::string payload_base64;
		/// Signature part decoded from base64
		std::string signature;
		/// Unmodified signature part in base64
		std::string signature_base64;
	public:
		/**
		 * Constructor 
		 * Parses a given token
		 * \param token The token to parse
		 * \throws std::invalid_argument Token is not in correct format
		 * \throws std::runtime_error Base64 decoding failed or invalid json
		 */
		explicit decoded_jwt(const std::string& token)
			: token(token)
		{
			auto hdr_end = token.find('.');
			if (hdr_end == std::string::npos)
				throw std::invalid_argument("invalid token supplied");
			auto payload_end = token.find('.', hdr_end + 1);
			if (payload_end == std::string::npos)
				throw std::invalid_argument("invalid token supplied");
			header = header_base64 = token.substr(0, hdr_end);
			payload = payload_base64 = token.substr(hdr_end + 1, payload_end - hdr_end - 1);
			signature = signature_base64 = token.substr(payload_end + 1);

			// Fix padding: JWT requires padding to get removed
			auto fix_padding = [](std::string& str) {
				switch (str.size() % 4) {
				case 1:
					str += alphabet::base64url::fill();
#ifdef __has_cpp_attribute
#if __has_cpp_attribute(fallthrough)
					[[fallthrough]];
#endif
#endif
				case 2:
					str += alphabet::base64url::fill();
#ifdef __has_cpp_attribute
#if __has_cpp_attribute(fallthrough)
					[[fallthrough]];
#endif
#endif
				case 3:
					str += alphabet::base64url::fill();
#ifdef __has_cpp_attribute
#if __has_cpp_attribute(fallthrough)
					[[fallthrough]];
#endif
#endif
				default:
					break;
				}
			};
			fix_padding(header);
			fix_padding(payload);
			fix_padding(signature);

			header = base::decode<alphabet::base64url>(header);
			payload = base::decode<alphabet::base64url>(payload);
			signature = base::decode<alphabet::base64url>(signature);

			auto parse_claims = [](const std::string& str) {
				std::unordered_map<std::string, claim> res;
				picojson::value val;
				if (!picojson::parse(val, str).empty())
					throw std::runtime_error("Invalid json");

				for (auto& e : val.get<picojson::object>()) { res.insert({ e.first, claim(e.second) }); }

				return res;
			};

			header_claims = parse_claims(header);
			payload_claims = parse_claims(payload);
		}

		/**
		 * Get token string, as passed to constructor
		 * \return token as passed to constructor
		 */
		const std::string& get_token() const noexcept { return token; }
		/**
		 * Get header part as json string
		 * \return header part after base64 decoding
		 */
		const std::string& get_header() const noexcept { return header; }
		/**
		 * Get payload part as json string
		 * \return payload part after base64 decoding
		 */
		const std::string& get_payload() const noexcept { return payload; }
		/**
		 * Get signature part as json string
		 * \return signature part after base64 decoding
		 */
		const std::string& get_signature() const noexcept { return signature; }
		/**
		 * Get header part as base64 string
		 * \return header part before base64 decoding
		 */
		const std::string& get_header_base64() const noexcept { return header_base64; }
		/**
		 * Get payload part as base64 string
		 * \return payload part before base64 decoding
		 */
		const std::string& get_payload_base64() const noexcept { return payload_base64; }
		/**
		 * Get signature part as base64 string
		 * \return signature part before base64 decoding
		 */
		const std::string& get_signature_base64() const noexcept { return signature_base64; }

	};

	/**
	 * Builder class to build and sign a new token
	 * Use jwt::create() to get an instance of this class.
	 */
	class builder {
		std::unordered_map<std::string, claim> header_claims;
		std::unordered_map<std::string, claim> payload_claims;

		builder() {}
		friend builder create();
	public:
		/**
		 * Set a header claim.
		 * \param id Name of the claim
		 * \param c Claim to add
		 * \return *this to allow for method chaining
		 */
		builder& set_header_claim(const std::string& id, claim c) { header_claims[id] = std::move(c); return *this; }
		/**
		 * Set a payload claim.
		 * \param id Name of the claim
		 * \param c Claim to add
		 * \return *this to allow for method chaining
		 */
		builder& set_payload_claim(const std::string& id, claim c) { payload_claims[id] = std::move(c); return *this; }
		/**
		 * Set algorithm claim
		 * You normally don't need to do this, as the algorithm is automatically set if you don't change it.
		 * \param str Name of algorithm
		 * \return *this to allow for method chaining
		 */
		builder& set_algorithm(const std::string& str) { return set_header_claim("alg", claim(str)); }
		/**
		 * Set type claim
		 * \param str Type to set
		 * \return *this to allow for method chaining
		 */
		builder& set_type(const std::string& str) { return set_header_claim("typ", claim(str)); }
		/**
		 * Set content type claim
		 * \param str Type to set
		 * \return *this to allow for method chaining
		 */
		builder& set_content_type(const std::string& str) { return set_header_claim("cty", claim(str)); }
		/**
		 * Set key id claim
		 * \param str Key id to set
		 * \return *this to allow for method chaining
		 */
		builder& set_key_id(const std::string& str) { return set_header_claim("kid", claim(str)); }
		/**
		 * Set issuer claim
		 * \param str Issuer to set
		 * \return *this to allow for method chaining
		 */
		builder& set_issuer(const std::string& str) { return set_payload_claim("iss", claim(str)); }
		/**
		 * Set subject claim
		 * \param str Subject to set
		 * \return *this to allow for method chaining
		 */
		builder& set_subject(const std::string& str) { return set_payload_claim("sub", claim(str)); }
		/**
		 * Set audience claim
		 * \param l Audience set
		 * \return *this to allow for method chaining
		 */
		builder& set_audience(const std::set<std::string>& l) { return set_payload_claim("aud", claim(l)); }
		/**
		 * Set audience claim
		 * \param aud Single audience
		 * \return *this to allow for method chaining
		 */
		builder& set_audience(const std::string& aud) { return set_payload_claim("aud", claim(aud)); }
		/**
		 * Set expires at claim
		 * \param d Expires time
		 * \return *this to allow for method chaining
		 */
		builder& set_expires_at(const date& d) { return set_payload_claim("exp", claim(d)); }
		/**
		 * Set not before claim
		 * \param d First valid time
		 * \return *this to allow for method chaining
		 */
		builder& set_not_before(const date& d) { return set_payload_claim("nbf", claim(d)); }
		/**
		 * Set issued at claim
		 * \param d Issued at time, should be current time
		 * \return *this to allow for method chaining
		 */
		builder& set_issued_at(const date& d) { return set_payload_claim("iat", claim(d)); }
		/**
		 * Set id claim
		 * \param str ID to set
		 * \return *this to allow for method chaining
		 */
		builder& set_id(const std::string& str) { return set_payload_claim("jti", claim(str)); }

		/**
		 * Sign token and return result
		 * \param algo Instance of an algorithm to sign the token with
		 * \return Final token as a string
		 */
		template<typename T>
		std::string sign(const T& algo) const {
			picojson::object obj_header;
			obj_header["alg"] = picojson::value(algo.name());
			for (auto& e : header_claims) {
				obj_header[e.first] = e.second.to_json();
			}
			picojson::object obj_payload;
			for (auto& e : payload_claims) {
				obj_payload.insert({ e.first, e.second.to_json() });
			}

			auto encode = [](const std::string& data) {
				auto base = base::encode<alphabet::base64url>(data);
				auto pos = base.find(alphabet::base64url::fill());
				base = base.substr(0, pos);
				return base;
			};

			std::string header = encode(picojson::value(obj_header).serialize());
			std::string payload = encode(picojson::value(obj_payload).serialize());

			std::string token = header + "." + payload;

			return token + "." + encode(algo.sign(token));
		}
	};

	/**
	 * Verifier class used to check if a decoded token contains all claims required by your application and has a valid signature.
	 */
	template<typename Clock>
	class verifier {
		struct algo_base {
			virtual ~algo_base() {}
			virtual void verify(const std::string& data, const std::string& sig) = 0;
		};
		template<typename T>
		struct algo : public algo_base {
			T alg;
			explicit algo(T a) : alg(a) {}
			virtual void verify(const std::string& data, const std::string& sig) override {
				alg.verify(data, sig);
			}
		};

		/// Required claims
		std::unordered_map<std::string, claim> claims;
		/// Leeway time for exp, nbf and iat
		size_t default_leeway = 0;
		/// Instance of clock type
		Clock clock;
		/// Supported algorithms
		std::unordered_map<std::string, std::shared_ptr<algo_base>> algs;
	public:
		/**
		 * Constructor for building a new verifier instance
		 * \param c Clock instance
		 */
		explicit verifier(Clock c) : clock(c) {}

		/**
		 * Set default leeway to use.
		 * \param leeway Default leeway to use if not specified otherwise
		 * \return *this to allow chaining
		 */
		verifier& leeway(size_t leeway) { default_leeway = leeway; return *this; }
		/**
		 * Set leeway for expires at.
		 * If not specified the default leeway will be used.
		 * \param leeway Set leeway to use for expires at.
		 * \return *this to allow chaining
		 */
		verifier& expires_at_leeway(size_t leeway) { return with_claim("exp", claim(std::chrono::system_clock::from_time_t(leeway))); }
		/**
		 * Set leeway for not before.
		 * If not specified the default leeway will be used.
		 * \param leeway Set leeway to use for not before.
		 * \return *this to allow chaining
		 */
		verifier& not_before_leeway(size_t leeway) { return with_claim("nbf", claim(std::chrono::system_clock::from_time_t(leeway))); }
		/**
		 * Set leeway for issued at.
		 * If not specified the default leeway will be used.
		 * \param leeway Set leeway to use for issued at.
		 * \return *this to allow chaining
		 */
		verifier& issued_at_leeway(size_t leeway) { return with_claim("iat", claim(std::chrono::system_clock::from_time_t(leeway))); }
		/**
		 * Set an issuer to check for.
		 * Check is casesensitive.
		 * \param iss Issuer to check for.
		 * \return *this to allow chaining
		 */
		verifier& with_issuer(const std::string& iss) { return with_claim("iss", claim(iss)); }
		/**
		 * Set a subject to check for.
		 * Check is casesensitive.
		 * \param sub Subject to check for.
		 * \return *this to allow chaining
		 */
		verifier& with_subject(const std::string& sub) { return with_claim("sub", claim(sub)); }
		/**
		 * Set an audience to check for.
		 * If any of the specified audiences is not present in the token the check fails.
		 * \param aud Audience to check for.
		 * \return *this to allow chaining
		 */
		verifier& with_audience(const std::set<std::string>& aud) { return with_claim("aud", claim(aud)); }
		/**
		 * Set an id to check for.
		 * Check is casesensitive.
		 * \param id ID to check for.
		 * \return *this to allow chaining
		 */
		verifier& with_id(const std::string& id) { return with_claim("jti", claim(id)); }
		/**
		 * Specify a claim to check for.
		 * \param name Name of the claim to check for
		 * \param c Claim to check for
		 * \return *this to allow chaining
		 */
		verifier& with_claim(const std::string& name, claim c) { claims[name] = c; return *this; }

		/**
		 * Add an algorithm available for checking.
		 * \param alg Algorithm to allow
		 * \return *this to allow chaining
		 */
		template<typename Algorithm>
		verifier& allow_algorithm(Algorithm alg) {
			algs[alg.name()] = std::make_shared<algo<Algorithm>>(alg);
			return *this;
		}

		/**
		 * Verify the given token.
		 * \param jwt Token to check
		 * \throws token_verification_exception Verification failed
		 */
		void verify(const decoded_jwt& jwt) const {
			const std::string data = jwt.get_header_base64() + "." + jwt.get_payload_base64();
			const std::string sig = jwt.get_signature();
			const std::string& algo = jwt.get_algorithm();
			if (algs.count(algo) == 0)
				throw token_verification_exception("wrong algorithm");
			algs.at(algo)->verify(data, sig);

			auto assert_claim_eq = [](const decoded_jwt& jwt, const std::string& key, const claim& c) {
				if (!jwt.has_payload_claim(key))
					throw token_verification_exception("decoded_jwt is missing " + key + " claim");
				auto& jc = jwt.get_payload_claim(key);
				if (jc.get_type() != c.get_type())
					throw token_verification_exception("claim " + key + " type mismatch");
				if (c.get_type() == claim::type::int64) {
					if (c.as_date() != jc.as_date())
						throw token_verification_exception("claim " + key + " does not match expected");
				}
				else if (c.get_type() == claim::type::array) {
					auto s1 = c.as_set();
					auto s2 = jc.as_set();
					if (s1.size() != s2.size())
						throw token_verification_exception("claim " + key + " does not match expected");
					auto it1 = s1.cbegin();
					auto it2 = s2.cbegin();
					while (it1 != s1.cend() && it2 != s2.cend()) {
						if (*it1++ != *it2++)
							throw token_verification_exception("claim " + key + " does not match expected");
					}
				}
				else if (c.get_type() == claim::type::string) {
					if (c.as_string() != jc.as_string())
						throw token_verification_exception("claim " + key + " does not match expected");
				}
				else throw token_verification_exception("internal error");
			};

			auto time = clock.now();

			if (jwt.has_expires_at()) {
				auto leeway = claims.count("exp") == 1 ? std::chrono::system_clock::to_time_t(claims.at("exp").as_date()) : default_leeway;
				auto exp = jwt.get_expires_at();
				if (time > exp + std::chrono::seconds(leeway))
					throw token_verification_exception("token expired");
			}
			if (jwt.has_issued_at()) {
				auto leeway = claims.count("iat") == 1 ? std::chrono::system_clock::to_time_t(claims.at("iat").as_date()) : default_leeway;
				auto iat = jwt.get_issued_at();
				if (time < iat - std::chrono::seconds(leeway))
					throw token_verification_exception("token expired");
			}
			if (jwt.has_not_before()) {
				auto leeway = claims.count("nbf") == 1 ? std::chrono::system_clock::to_time_t(claims.at("nbf").as_date()) : default_leeway;
				auto nbf = jwt.get_not_before();
				if (time < nbf - std::chrono::seconds(leeway))
					throw token_verification_exception("token expired");
			}
			for (auto& c : claims)
			{
				if (c.first == "exp" || c.first == "iat" || c.first == "nbf") {
					// Nothing to do here, already checked
				}
				else if (c.first == "aud") {
					if (!jwt.has_audience())
						throw token_verification_exception("token doesn't contain the required audience");
					auto aud = jwt.get_audience();
					auto expected = c.second.as_set();
					for (auto& e : expected)
						if (aud.count(e) == 0)
							throw token_verification_exception("token doesn't contain the required audience");
				}
				else {
					assert_claim_eq(jwt, c.first, c.second);
				}
			}
		}
	};

	/**
	 * Create a verifier using the given clock
	 * \param c Clock instance to use
	 * \return verifier instance
	 */
	template<typename Clock>
	verifier<Clock> verify(Clock c) {
		return verifier<Clock>(c);
	}

	/**
	 * Default clock class using std::chrono::system_clock as a backend.
	 */
	struct default_clock {
		std::chrono::system_clock::time_point now() const {
			return std::chrono::system_clock::now();
		}
	};

	/**
	 * Create a verifier using the default clock
	 * \return verifier instance
	 */
    inline
	verifier<default_clock> verify() {
		return verify<default_clock>({});
	}

	/**
	 * Return a builder instance to create a new token
	 */
    inline
	builder create() {
		return builder();
	}

	/**
	 * Decode a token
	 * \param token Token to decode
	 * \return Decoded token
	 * \throws std::invalid_argument Token is not in correct format
	 * \throws std::runtime_error Base64 decoding failed or invalid json
	 */
    inline
	decoded_jwt decode(const std::string& token) {
		return decoded_jwt(token);
	}
}

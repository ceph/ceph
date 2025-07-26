# RESTler Fuzzing Tool

[RESTler](https://github.com/microsoft/restler-fuzzer) is a **stateful REST API fuzzing tool** developed by Microsoft Research. It automatically tests REST APIs by generating sequences of requests that follow the API's logical usage patterns. RESTler analyzes the API specification to understand dependencies between requests and builds complex request sequences that can uncover deep bugs in backend systems.

Unlike simple fuzzers that test individual endpoints in isolation, RESTler performs **stateful fuzzing**, meaning it understands and preserves state across sequences of API calls — such as creating a resource and later updating or deleting it. This makes it especially effective for detecting bugs related to:

- Resource lifecycle (create, update, delete)
- Authorization and access control
- Workflow and sequencing logic

---

## 📄 API Specification Document Required by RESTler

RESTler requires an **OpenAPI specification document** to operate. This specification describes the structure of the API, including:

- Endpoints (paths and HTTP methods)
- Request parameters and schemas
- Expected responses
- Authentication methods
- Data types and format constraints

RESTler supports:
- **OpenAPI 2.0 (Swagger)** or **OpenAPI 3.0+**
- File format: **JSON** or **YAML**

The API specification is used by RESTler's compiler to:

- Parse the API definition
- Generate a grammar defining valid request sequences
- Produce test sequences that respect API dependencies and constraints

---

## 🧾 OpenAPI Specification Used

The file [`s3_bucket.json`](./s3_bucket.json) is a custom OpenAPI specification tailored for **Ceph RGW (RADOS Gateway)**.

---

## 🚀 Steps to Fuzz Ceph RGW with RESTler

### 1. Install RESTler

Follow the official setup instructions provided in the [RESTler GitHub repository](https://github.com/microsoft/restler-fuzzer?tab=readme-ov-file#setting-up-restler).

### 2. Compile the OpenAPI Specification

Once RESTler is set up and built, compile the API spec using the following command:

```bash
restler_bin/restler/Restler compile --api_spec s3_bucket.json
```

### 3. Test the Grammar

After compilation, test the generated grammar to verify that all endpoints are reachable:
```bash
restler_bin/restler/Restler test \
  --grammar_file Compile/grammar.py \
  --dictionary_file Compile/dict.json \
  --settings engine_settings.json \
  --no_ssl
```

Note: Ensure that engine_settings.json is configured correctly. You can refer to this example: [engine settings reference](https://github.com/microsoft/restler-fuzzer/pull/962)

### 4. Start with Fuzzing

Once all endpoints are reachable, initiate the fuzzing process.

## 📚 References

[RESTler GitHub Repository](https://github.com/microsoft/restler-fuzzer)


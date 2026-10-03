//go:build ignore

// Bucket policy enforcement test — implements test-policy-plan.md groups 1-9.
// Hits frontends on :9081 and :9082 directly (never nginx :9080). Needs N>=2.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	endpoint  = "http://127.0.0.1:9081"
	endpoint2 = "http://127.0.0.1:9082"
	bucket    = "policy-test"
	failures  int
)

func main() {
	if len(os.Args) > 1 {
		endpoint = os.Args[1]
	}
	if len(os.Args) > 2 {
		endpoint2 = os.Args[2]
	}
	if e := os.Getenv("ENDPOINT"); e != "" {
		endpoint = e
	}
	if e := os.Getenv("ENDPOINT2"); e != "" {
		endpoint2 = e
	}

	requireLive(endpoint)
	requireLive(endpoint2)

	setup()
	group1()
	group2()
	group3()
	group4()
	group5()
	group6()
	group7()
	group8()
	group9()

	fmt.Printf("\n=== Policy test complete: %d failures ===\n", failures)
	if failures > 0 {
		os.Exit(1)
	}
}

func setup() {
	fmt.Println("=== Setup ===")
	createBucket(endpoint, bucket)
	if rc := put(endpoint, bucket, "obj-a", "data-a"); rc != 200 {
		fatal("setup PUT obj-a: %d", rc)
	}
	if rc := put(endpoint, bucket, "obj-b", "data-b"); rc != 200 {
		fatal("setup PUT obj-b: %d", rc)
	}
	if rc := put(endpoint, bucket, "obj-c", "data-c"); rc != 200 {
		fatal("setup PUT obj-c: %d", rc)
	}
}

const denyWrite = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":["s3:PutObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::policy-test/*"}]}`
const denyList = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::policy-test"}]}`
const denyRead = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::policy-test/*"}]}`
const denyDeleteBucket = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:DeleteBucket","Resource":"arn:aws:s3:::policy-test"}]}`

func putPolicy(json string) {
	out, err := aws(endpoint, "s3api", "put-bucket-policy", "--bucket", bucket, "--policy", json)
	if err != nil {
		fatal("PutBucketPolicy: %s %v", out, err)
	}
}

func deletePolicy() {
	out, err := aws(endpoint, "s3api", "delete-bucket-policy", "--bucket", bucket)
	if err != nil {
		fatal("DeleteBucketPolicy: %s %v", out, err)
	}
}

func createBucket(ep, name string) {
	out, err := aws(ep, "s3api", "create-bucket", "--bucket", name)
	if err != nil && !strings.Contains(out, "BucketAlreadyOwnedByYou") && !strings.Contains(out, "BucketAlreadyExists") {
		fatal("create-bucket %s: %s %v", name, out, err)
	}
}

func requireLive(ep string) {
	out, err := aws(ep, "s3api", "list-buckets")
	if err != nil {
		fatal("cannot reach %s (need 2 frontends, e.g. ./scripts/reload.sh --clean 2): %s %v", ep, out, err)
	}
}

func aws(ep string, args ...string) (string, error) {
	allArgs := append([]string{"--endpoint-url", ep}, args...)
	cmd := exec.Command("aws", allArgs...)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_DEFAULT_REGION=us-east-1",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func codeFromAWS(out string, err error) int {
	if err == nil {
		if strings.Contains(out, "AccessDenied") {
			return 403
		}
		return 200
	}
	if strings.Contains(out, "AccessDenied") || strings.Contains(out, "Forbidden") || strings.Contains(out, "(403)") {
		return 403
	}
	return 500
}

func put(ep, bkt, key, data string) int {
	cmd := exec.Command("aws", "--endpoint-url", ep, "s3", "cp", "-", "s3://"+bkt+"/"+key)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_DEFAULT_REGION=us-east-1",
	)
	cmd.Stdin = strings.NewReader(data)
	out, err := cmd.CombinedOutput()
	return codeFromAWS(string(out), err)
}

func get(ep, bkt, key string) int {
	out, err := aws(ep, "s3", "cp", "s3://"+bkt+"/"+key, "-")
	return codeFromAWS(out, err)
}

func head(ep, bkt, key string) int {
	out, err := aws(ep, "s3api", "head-object", "--bucket", bkt, "--key", key)
	return codeFromAWS(out, err)
}

func list(ep, bkt string) int {
	out, err := aws(ep, "s3api", "list-objects-v2", "--bucket", bkt)
	return codeFromAWS(out, err)
}

func del(ep, bkt, key string) int {
	out, err := aws(ep, "s3api", "delete-object", "--bucket", bkt, "--key", key)
	return codeFromAWS(out, err)
}

func deleteMulti(ep, bkt string, keys ...string) int {
	objects := `{"Objects":[`
	for i, k := range keys {
		if i > 0 {
			objects += ","
		}
		objects += fmt.Sprintf(`{"Key":"%s"}`, k)
	}
	objects += `]}`
	out, err := aws(ep, "s3api", "delete-objects", "--bucket", bkt, "--delete", objects)
	return codeFromAWS(out, err)
}

func deleteBucket(ep, bkt string) int {
	out, err := aws(ep, "s3api", "delete-bucket", "--bucket", bkt)
	return codeFromAWS(out, err)
}

func expect(name string, got, want int) {
	if got == want {
		fmt.Printf("  PASS: %s → %d\n", name, got)
	} else {
		fmt.Printf("  FAIL: %s → %d (want %d)\n", name, got, want)
		failures++
	}
}

func group1() {
	fmt.Println("\n=== Group 1: Deny Write — PUT rejected ===")
	putPolicy(denyWrite)
	expect("PUT", put(endpoint, bucket, "new-obj", "x"), 403)
	expect("GET", get(endpoint, bucket, "obj-a"), 200)
	expect("HEAD", head(endpoint, bucket, "obj-a"), 200)
	expect("LIST", list(endpoint, bucket), 200)
	deletePolicy()
}

func group2() {
	fmt.Println("\n=== Group 2: Deny Write — DELETE rejected ===")
	putPolicy(denyWrite)
	expect("DELETE", del(endpoint, bucket, "obj-a"), 403)
	expect("GET", get(endpoint, bucket, "obj-a"), 200)
	expect("HEAD", head(endpoint, bucket, "obj-a"), 200)
	expect("LIST", list(endpoint, bucket), 200)
	deletePolicy()
}

func group3() {
	fmt.Println("\n=== Group 3: Deny Write — DELETE-multi rejected ===")
	putPolicy(denyWrite)
	expect("DELETE-multi", deleteMulti(endpoint, bucket, "obj-a", "obj-b", "obj-c"), 403)
	expect("GET", get(endpoint, bucket, "obj-a"), 200)
	expect("HEAD", head(endpoint, bucket, "obj-a"), 200)
	expect("LIST", list(endpoint, bucket), 200)
	deletePolicy()
}

func group4() {
	fmt.Println("\n=== Group 4: Deny List — LIST rejected, others pass ===")
	putPolicy(denyList)
	expect("LIST", list(endpoint, bucket), 403)
	expect("GET", get(endpoint, bucket, "obj-a"), 200)
	expect("HEAD", head(endpoint, bucket, "obj-a"), 200)
	expect("PUT", put(endpoint, bucket, "obj-tmp", "x"), 200)
	expect("DELETE", del(endpoint, bucket, "obj-tmp"), 200)
	expect("DELETE-multi", deleteMulti(endpoint, bucket, "obj-a"), 200)
	deletePolicy()
	put(endpoint, bucket, "obj-a", "data-a")
}

func group5() {
	fmt.Println("\n=== Group 5: Deny Read — reads rejected, writes/list pass ===")
	putPolicy(denyRead)
	fmt.Println("  sleeping 4s for cache TTL...")
	time.Sleep(4 * time.Second)
	expect("GET", get(endpoint, bucket, "obj-a"), 403)
	expect("HEAD", head(endpoint, bucket, "obj-a"), 403)
	expect("PUT", put(endpoint, bucket, "obj-tmp", "x"), 200)
	expect("DELETE", del(endpoint, bucket, "obj-tmp"), 200)
	expect("DELETE-multi", deleteMulti(endpoint, bucket, "obj-b"), 200)
	expect("LIST", list(endpoint, bucket), 200)
	deletePolicy()
	put(endpoint, bucket, "obj-b", "data-b")
}

func group6() {
	fmt.Println("\n=== Group 6: Deny Read — GET cross-instance TTL + refresh-before-reject ===")
	expect("GET warmup on instance2", get(endpoint2, bucket, "obj-a"), 200)
	putPolicy(denyRead)
	expect("GET (immediate, instance1)", get(endpoint, bucket, "obj-a"), 403)
	expect("GET (stale cache, instance2)", get(endpoint2, bucket, "obj-a"), 200)
	fmt.Println("  sleeping 4s for cache TTL...")
	time.Sleep(4 * time.Second)
	expect("GET (after TTL, instance2)", get(endpoint2, bucket, "obj-a"), 403)
	deletePolicy()
	expect("GET (immediate allow, instance1)", get(endpoint, bucket, "obj-a"), 200)
	expect("GET (refresh-before-reject, instance2)", get(endpoint2, bucket, "obj-a"), 200)
}

func group7() {
	fmt.Println("\n=== Group 7: Deny Read — HEAD cross-instance TTL + refresh-before-reject ===")
	expect("HEAD warmup on instance2", head(endpoint2, bucket, "obj-a"), 200)
	putPolicy(denyRead)
	expect("HEAD (immediate, instance1)", head(endpoint, bucket, "obj-a"), 403)
	expect("HEAD (stale cache, instance2)", head(endpoint2, bucket, "obj-a"), 200)
	fmt.Println("  sleeping 4s for cache TTL...")
	time.Sleep(4 * time.Second)
	expect("HEAD (after TTL, instance2)", head(endpoint2, bucket, "obj-a"), 403)
	deletePolicy()
	expect("HEAD (immediate allow, instance1)", head(endpoint, bucket, "obj-a"), 200)
	expect("HEAD (refresh-before-reject, instance2)", head(endpoint2, bucket, "obj-a"), 200)
}

func group8() {
	fmt.Println("\n=== Group 8: Deny DeleteBucket ===")
	del(endpoint, bucket, "obj-a")
	del(endpoint, bucket, "obj-b")
	del(endpoint, bucket, "obj-c")
	putPolicy(denyDeleteBucket)
	expect("DELETE bucket", deleteBucket(endpoint, bucket), 403)
	deletePolicy()
	expect("DELETE bucket (no policy)", deleteBucket(endpoint, bucket), 200)
}

func group9() {
	fmt.Println("\n=== Group 9: No policy sanity ===")
	sanityBucket := "policy-sanity"
	createBucket(endpoint, sanityBucket)
	expect("PUT", put(endpoint, sanityBucket, "x", "data"), 200)
	expect("GET", get(endpoint, sanityBucket, "x"), 200)
	expect("HEAD", head(endpoint, sanityBucket, "x"), 200)
	expect("LIST", list(endpoint, sanityBucket), 200)
	expect("DELETE", del(endpoint, sanityBucket, "x"), 200)
	expect("DELETE bucket", deleteBucket(endpoint, sanityBucket), 200)
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(2)
}

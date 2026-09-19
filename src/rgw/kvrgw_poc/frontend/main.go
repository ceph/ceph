package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"github.com/gofiber/fiber/v3"
	"github.com/kv-poc/frontend/kvrgwc"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
)

const defaultTenantName = "kv-poc"
const defaultGoMaxProcs = 8

func defaultTenantFromEnv() string {
	if name := os.Getenv("KVRGW_TENANT_NAME"); name != "" {
		return name
	}
	return defaultTenantName
}

func goMaxProcsFromEnv() int {
	raw := os.Getenv("KVRGW_GOMAXPROCS")
	if raw == "" {
		return defaultGoMaxProcs
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		log.Printf("invalid KVRGW_GOMAXPROCS=%q, using %d", raw, defaultGoMaxProcs)
		return defaultGoMaxProcs
	}
	return n
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	addr := flag.String("addr", ":9080", "S3 API listen address")
	flag.Parse()

	runtime.GOMAXPROCS(goMaxProcsFromEnv())

	handle, ec := kvrgwc.Start(os.Getenv("KVRGW_DATA"))
	if ec != kvrgwc.ErrOK {
		log.Fatalf("kvrgw start: error_code=%d", ec)
	}
	defer handle.Stop()

	rootAccess := envOrDefault("ROOT_ACCESS_KEY", "test")
	rootSecret := envOrDefault("ROOT_SECRET_KEY", "test")
	iamDir := os.Getenv("KVRGW_IAM_DIR")

	kvBe := NewKvRgwBackend(handle, defaultTenantFromEnv(), rootAccess)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	kvBe.EnsureTenant(ctx)

	cfg := &embedgw.Config{
		RootUserAccess:           rootAccess,
		RootUserSecret:           rootSecret,
		Region:                   envOrDefault("AWS_DEFAULT_REGION", "us-east-1"),
		Ports:                    []string{*addr},
		MaxConnections:           4096,
		MaxRequests:              4096,
		MultipartMaxParts:        10000,
		DisableStrictBucketNames: true,
		DisableACLs:              true,
		IAMDir:                   iamDir,
		IAMCacheTTL:              120,
		IAMCachePrune:            3600,
		Debug:                    os.Getenv("KVRGW_DEBUG") == "1",
		S3Options: []s3api.Option{
			s3api.WithRoute(http.MethodPut, "/_admin/tenant/:name", adminAddTenantHandler(kvBe)),
		},
	}

	log.Printf("kv-rgw frontend listening on %s (cgo, tenant=%s, GOMAXPROCS=%d)",
		*addr, kvBe.tenantName(), runtime.GOMAXPROCS(0))

	if err := embedgw.RunVersityGW(ctx, kvBe, cfg); err != nil {
		if ctx.Err() != nil {
			log.Printf("gateway shutdown: %v", err)
		} else {
			log.Fatalf("gateway: %v", err)
		}
	}
}

func adminAddTenantHandler(be *KvRgwBackend) fiber.Handler {
	return func(c fiber.Ctx) error {
		name := c.Params("name")
		if name == "" {
			return c.Status(http.StatusBadRequest).SendString("missing tenant name")
		}
		tenantID, err := be.AddTenant(c.Context(), name)
		if err != nil {
			return c.Status(http.StatusInternalServerError).SendString(err.Error())
		}
		c.Set("Content-Type", "text/plain")
		return c.Status(http.StatusOK).SendString(fmt.Sprintf("%d", tenantID))
	}
}

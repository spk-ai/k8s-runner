package workloadproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/enroll"
)

// EnrollFunc turns a one-time enrollment token into an identity document.
type EnrollFunc func(ctx context.Context, token string) ([]byte, error)

// ProxyEnvVars are the variables Go's ProxyFromEnvironment (and the SDK's
// enrollment and controller clients) read. Overlay containers must never route
// controller traffic into the workload proxy they are bringing up.
var ProxyEnvVars = []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"}

// EnrollIdentity writes the workload's identity once, from its enrollment
// token, to path. It needs no capability and edits no resolver or hosts file:
// the controller named by the token's issuer is reached through the Pod's
// ordinary cluster DNS. An identity that already exists is kept, so a
// restarted Pod sandbox never spends the (single-use) token twice.
//
// The identity is written 0600 through a same-directory rename, so the sidecar
// never reads a partial file. Neither the token nor key material is logged.
func EnrollIdentity(ctx context.Context, path, token string, enrollFn EnrollFunc) error {
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return nil
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return errors.New("enrollment token is required")
	}
	identity, err := enrollFn(ctx, token)
	if err != nil {
		return errors.New(redact(err.Error(), token))
	}
	if len(identity) == 0 {
		return errors.New("enrollment produced no identity")
	}
	dir := filepath.Dir(path)
	temporary, err := os.CreateTemp(dir, ".identity-*")
	if err != nil {
		return fmt.Errorf("create identity: %w", err)
	}
	defer os.Remove(temporary.Name())
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect identity: %w", err)
	}
	if _, err := temporary.Write(identity); err != nil {
		temporary.Close()
		return fmt.Errorf("write identity: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync identity: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close identity: %w", err)
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("install identity: %w", err)
	}
	return nil
}

func redact(message, token string) string {
	if token == "" {
		return message
	}
	return strings.ReplaceAll(message, token, "[redacted]")
}

// ZitiEnroll enrolls with the OpenZiti SDK. The token's signature is checked
// against the certificate its own https issuer presents, then the controller's
// CA bundle is fetched and pinned in the identity, as `ziti edge enroll` does.
// Reading the issuer is retried for the bounded startup window in which the
// controller's Service may not resolve yet; the enrollment POST itself is not
// retried, because the token is single-use.
func ZitiEnroll(ctx context.Context, token string) ([]byte, error) {
	for _, name := range ProxyEnvVars {
		_ = os.Unsetenv(name)
	}
	var claims *ziti.EnrollmentClaims
	var err error
	deadline := time.Now().Add(60 * time.Second)
	for {
		claims, _, err = enroll.ParseToken(token)
		if err == nil || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		return nil, fmt.Errorf("verify enrollment token: %v", err)
	}
	issuer, err := url.Parse(claims.Issuer)
	if err != nil || issuer.Scheme != "https" || issuer.Host == "" {
		return nil, errors.New("enrollment token issuer must be an https controller URL")
	}
	keyAlg := ziti.KeyAlgVar("EC")
	config, err := enroll.Enroll(enroll.EnrollmentFlags{Token: claims, KeyAlg: keyAlg})
	if err != nil {
		return nil, fmt.Errorf("enroll: %v", err)
	}
	return json.Marshal(config)
}

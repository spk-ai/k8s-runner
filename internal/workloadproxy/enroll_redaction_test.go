package workloadproxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeController is just enough of an OpenZiti controller for the SDK's ott
// enrollment to reach its enrollment POST: the issuer answers TLS, publishes
// its own certificate as the well-known CA bundle, and then drops the POST,
// which is what a reset, timeout or TLS failure on the controller looks like.
func fakeController(t *testing.T) (*httptest.Server, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fake-controller"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /edge/client/v1/.well-known/est/cacerts", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/pkcs7-mime")
		_, _ = w.Write([]byte(base64.StdEncoding.EncodeToString(degeneratePKCS7(t, der))))
	})
	mux.HandleFunc("POST /edge/client/v1/enroll", func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server := httptest.NewUnstartedServer(mux)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, key
}

// degeneratePKCS7 is a certs-only SignedData, the shape of an EST cacerts reply.
func degeneratePKCS7(t *testing.T, cert []byte) []byte {
	t.Helper()
	type contentInfo struct {
		ContentType asn1.ObjectIdentifier
		Content     asn1.RawValue `asn1:"explicit,optional,tag:0"`
	}
	type signedData struct {
		Version          int
		DigestAlgorithms []pkix.AlgorithmIdentifier `asn1:"set"`
		ContentInfo      contentInfo
		Certificates     asn1.RawValue   `asn1:"optional,tag:0"`
		SignerInfos      []asn1.RawValue `asn1:"set"`
	}
	inner, err := asn1.Marshal(signedData{
		Version:      1,
		ContentInfo:  contentInfo{ContentType: asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 1}},
		Certificates: asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: cert},
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := asn1.Marshal(contentInfo{
		ContentType: asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 2},
		Content:     asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: inner},
	})
	if err != nil {
		t.Fatal(err)
	}
	return outer
}

// enrollmentJWT signs an ott enrollment token the way the controller does,
// with the key behind the issuer's TLS certificate.
func enrollmentJWT(t *testing.T, issuer, jti string, key *ecdsa.PrivateKey) string {
	t.Helper()
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(data)
	}
	signingInput := encode(map[string]string{"alg": "ES256", "typ": "JWT"}) + "." +
		encode(map[string]any{"iss": issuer, "sub": "workload", "jti": jti, "em": "ott", "exp": time.Now().Add(time.Hour).Unix()})
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	signature := make([]byte, 64)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// The jti of an ott enrollment JWT is the one-time token itself: the
// controller enrolls whoever POSTs it with a CSR. The SDK carries it in the
// enrollment URL's query, and a transport failure on that POST returns a
// *url.Error that prints the URL. The failed POST did not spend the token, so
// the error the enroll container logs must not carry it.
func TestZitiEnrollFailureNeverExposesTheOneTimeToken(t *testing.T) {
	for _, name := range ProxyEnvVars {
		t.Setenv(name, "") // ZitiEnroll unsets them; restore the test process afterwards
	}
	controller, key := fakeController(t)
	const jti = "6d1f0c52-one-time-enrollment-token"
	token := enrollmentJWT(t, controller.URL, jti, key)
	path := filepath.Join(t.TempDir(), "agent.json")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := EnrollIdentity(ctx, path, token, ZitiEnroll)
	if err == nil {
		t.Fatal("enrollment against a controller that drops the POST succeeded")
	}
	if !strings.Contains(err.Error(), "/edge/client/v1/enroll") {
		t.Fatalf("failure is not the enrollment POST, so this test proves nothing: %v", err)
	}
	if strings.Contains(err.Error(), jti) || strings.Contains(err.Error(), token) {
		t.Fatalf("enrollment error exposes the one-time token: %v", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("failed enrollment left an identity")
	}
}

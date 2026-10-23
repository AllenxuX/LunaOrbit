package registry

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

func signedSession(user string, expires time.Time, key []byte) string {
	payload := user + "." + strconv.FormatInt(expires.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func sessionActor(token string, key []byte) (Actor, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || !registrationID.MatchString(parts[0]) {
		return Actor{}, false
	}
	expires, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().Unix() >= expires || expires > time.Now().Add(8*time.Hour+time.Minute).Unix() {
		return Actor{}, false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Actor{}, false
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return Actor{}, false
	}
	return Actor{ID: parts[0], Role: "admin"}, true
}

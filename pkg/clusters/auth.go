// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clusters

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

const (
	// GoogleAuthEnv configures how leeway authenticates Google API discovery and
	// GKE control-plane requests when no credential proxy is active. Accepted
	// values are "adc" (or "default") and "injected".
	GoogleAuthEnv = "LEEWAY_GOOGLE_AUTH" // #nosec G101 -- environment variable name, not a credential

	// CoreAgentGoogleAuthEnv and MastGoogleAuthEnv are the Substrate task
	// environment switches set by core-agent and mast when an external egress
	// gateway injects the real Google bearer token in place of a placeholder.
	CoreAgentGoogleAuthEnv = "CORE_AGENT_GOOGLE_AUTH" // #nosec G101 -- environment variable name, not a credential
	MastGoogleAuthEnv      = "MAST_GOOGLE_AUTH"       // #nosec G101 -- environment variable name, not a credential

	// GoogleAuthADC resolves Application Default Credentials (the default).
	GoogleAuthADC = "adc"
	// GoogleAuthInjected sends a static placeholder bearer token without
	// consulting ADC or the GCE metadata server.
	GoogleAuthInjected = "injected"

	// InjectedTokenValue is the static placeholder bearer sent under
	// GoogleAuthInjected so the Substrate egress gateway swaps in the task's
	// real Google OAuth token.
	InjectedTokenValue = "placeholder"
)

// ResolveGoogleAuthMode resolves the effective Google auth mode from an
// explicit flag value and os.Getenv.
func ResolveGoogleAuthMode(explicit string) (string, error) {
	return ResolveGoogleAuthModeWith(explicit, os.Getenv)
}

// ResolveGoogleAuthModeWith resolves the effective Google auth mode using
// getenv (falling back to os.Getenv when nil).
//
// Precedence:
//  1. Explicit flag value ("adc", "default", or "injected").
//  2. LEEWAY_GOOGLE_AUTH ("adc", "default", or "injected").
//  3. CORE_AGENT_GOOGLE_AUTH=injected or MAST_GOOGLE_AUTH=injected (other
//     values on those foreign variables are ignored so host-specific settings
//     do not break leeway).
//  4. GoogleAuthADC.
func ResolveGoogleAuthModeWith(explicit string, getenv func(string) string) (string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if v := strings.ToLower(strings.TrimSpace(explicit)); v != "" {
		switch v {
		case GoogleAuthADC, "default":
			return GoogleAuthADC, nil
		case GoogleAuthInjected:
			return GoogleAuthInjected, nil
		default:
			return "", fmt.Errorf("unknown Google auth mode %q (want %q or %q)", explicit, GoogleAuthADC, GoogleAuthInjected)
		}
	}
	if v := strings.ToLower(strings.TrimSpace(getenv(GoogleAuthEnv))); v != "" {
		switch v {
		case GoogleAuthADC, "default":
			return GoogleAuthADC, nil
		case GoogleAuthInjected:
			return GoogleAuthInjected, nil
		default:
			return "", fmt.Errorf("%s=%q: unknown Google auth mode (want %q or %q)", GoogleAuthEnv, v, GoogleAuthADC, GoogleAuthInjected)
		}
	}
	if strings.EqualFold(strings.TrimSpace(getenv(CoreAgentGoogleAuthEnv)), GoogleAuthInjected) ||
		strings.EqualFold(strings.TrimSpace(getenv(MastGoogleAuthEnv)), GoogleAuthInjected) {
		return GoogleAuthInjected, nil
	}
	return GoogleAuthADC, nil
}

// InjectedTokenSource returns a static TokenSource carrying InjectedTokenValue.
func InjectedTokenSource() oauth2.TokenSource {
	return oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: InjectedTokenValue,
		TokenType:   "Bearer",
	})
}

// googleAPIOptionsForTokenSource builds google.golang.org/api client options
// that authenticate exclusively with ts and bypass Application Default
// Credentials and GCE metadata lookups completely.
func googleAPIOptionsForTokenSource(ts oauth2.TokenSource, endpoint string) []option.ClientOption {
	opts := []option.ClientOption{
		option.WithHTTPClient(&http.Client{
			Transport: &oauth2.Transport{
				Source: ts,
				Base:   http.DefaultTransport,
			},
		}),
		option.WithoutAuthentication(),
	}
	if strings.TrimSpace(endpoint) != "" {
		opts = append(opts, option.WithEndpoint(strings.TrimSpace(endpoint)))
	}
	return opts
}

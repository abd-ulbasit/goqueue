package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The broker has no config file and no flags, so a GOQUEUE_* environment
// variable is the whole of its configuration surface. That makes a particular
// mistake invisible: a deployment manifest can set a variable nothing reads,
// and the broker starts happily with the default. Nothing errors, nothing logs,
// and the operator believes a setting was applied.
//
// That had happened to five variables in the Helm chart and nine in the Compose
// stack (log level and format, metrics toggle, the whole tracing block,
// replication factor and minimum ISR), plus a backup CronJob that passed
// GOQUEUE_BROKER_ADDRESS to a binary reading GOQUEUE_ADMIN_SERVER and so
// silently fell back to http://localhost:8080.
//
// These tests close that gap by deriving the set of variables the code actually
// reads and asserting every manifest stays inside it.

var (
	envNamePattern   = regexp.MustCompile(`^GOQUEUE_[A-Z0-9_]*$`)
	envSuffixPattern = regexp.MustCompile(`^_[A-Z0-9_]+$`)
)

// repoRoot returns the module root, two levels up from internal/config.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("could not locate module root from %s: %v", wd, err)
	}
	return root
}

// readableEnvVars returns every GOQUEUE_* name the non-test Go sources could
// plausibly consult.
//
// It collects string literals from the AST rather than matching os.Getenv call
// sites directly, because names reach os.Getenv by several routes: literally
// (os.Getenv("GOQUEUE_CLUSTER_ENABLED")), through wrappers
// (getEnvOrDefault("GOQUEUE_LISTENERS_INTERNAL", ":7000")), through package
// constants (internal/cli), and through a prefix parameter that is concatenated
// with a suffix inside the callee (internal/security/tls.go does
// os.Getenv(prefix+"_CERT_FILE"), so the full name is never one literal). The
// last case is covered by crossing every name with every suffix.
//
// Scanning literals over-approximates: a name that is written down but no
// longer consulted still counts as readable. That direction is deliberate. This
// set gates a test failure, so it must never invent one, and the check it backs
// is still sharp enough to catch a variable that exists only in a manifest.
// Comments are not literals, so prose naming a variable does not count.
func readableEnvVars(t *testing.T, root string) map[string]bool {
	t.Helper()

	names := map[string]bool{}
	suffixes := map[string]bool{}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "website", "memory-bank", "bin", "data", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return nil // unparseable generated code is not this test's business
		}

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			switch {
			case envNamePattern.MatchString(s):
				names[s] = true
			case envSuffixPattern.MatchString(s):
				suffixes[s] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	if len(names) == 0 {
		t.Fatal("found no GOQUEUE_* literals in the Go sources; the scan is broken, not the manifests")
	}

	for name := range names {
		for suffix := range suffixes {
			names[name+suffix] = true
		}
	}
	return names
}

// manifestEnvVars collects every GOQUEUE_* name a deployment manifest injects
// into a container's environment, keyed by name to the files that set it.
//
// Only the two forms that actually define a container variable count: the
// Kubernetes "- name: GOQUEUE_X" entry and the Compose "- GOQUEUE_X=value"
// entry. Shell scripts under deploy/ are excluded on purpose; their GOQUEUE_URL
// and GOQUEUE_PODS are the script's own variables, never passed to a goqueue
// process, and counting them would produce failures with nothing behind them.
//
// Containers are filtered the same way. A GOQUEUE_-prefixed variable set on a
// sidecar is that sidecar's business: the benchmark Job hands GOQUEUE_URL to a
// python image, which no amount of Go code will ever read and which is entirely
// correct. Only env belonging to a container whose image or command names a
// goqueue binary is checked.
func manifestEnvVars(t *testing.T, root string) map[string][]string {
	t.Helper()

	k8sEnv := regexp.MustCompile(`^-\s+name:\s+(GOQUEUE_[A-Z0-9_]+)\s*$`)
	composeEnv := regexp.MustCompile(`^-\s+(GOQUEUE_[A-Z0-9_]+)=`)
	imageLine := regexp.MustCompile(`^-?\s*image:\s*(.*)$`)

	refs := map[string][]string{}
	deployDir := filepath.Join(root, "deploy")

	err := filepath.Walk(deployDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		switch filepath.Ext(path) {
		case ".yaml", ".yml", ".tpl":
		default:
			return nil
		}
		if strings.HasSuffix(path, ".bak") {
			return nil
		}

		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)

		// Tracks whether the container currently being described runs goqueue.
		// An image line re-decides it; a command naming a goqueue binary turns
		// it on, which is how the backup CronJob (a generic image running
		// goqueue-admin) is recognised.
		goqueueContainer := false

		for _, line := range strings.Split(string(body), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue // prose explains variables without setting them
			}

			if m := imageLine.FindStringSubmatch(trimmed); m != nil {
				goqueueContainer = strings.Contains(strings.ToLower(m[1]), "goqueue")
				continue
			}
			if strings.Contains(trimmed, "goqueue-admin") || strings.Contains(trimmed, "/goqueue") {
				goqueueContainer = true
			}
			if !goqueueContainer {
				continue
			}

			for _, re := range []*regexp.Regexp{k8sEnv, composeEnv} {
				if m := re.FindStringSubmatch(trimmed); m != nil {
					refs[m[1]] = appendUnique(refs[m[1]], rel)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", deployDir, err)
	}
	return refs
}

func appendUnique(s []string, v string) []string {
	for _, e := range s {
		if e == v {
			return s
		}
	}
	return append(s, v)
}

// TestDeploymentManifestsOnlySetVariablesTheCodeReads fails when a manifest
// configures the broker through a variable no code path consults. Such a
// variable is not a harmless leftover: it is a control that looks wired up and
// is not, and it fails silently every time someone changes it.
func TestDeploymentManifestsOnlySetVariablesTheCodeReads(t *testing.T) {
	root := repoRoot(t)
	readable := readableEnvVars(t, root)
	referenced := manifestEnvVars(t, root)

	if len(referenced) == 0 {
		t.Fatal("found no GOQUEUE_* references in deploy/; the scan is broken, not the manifests")
	}

	var dead []string
	for name := range referenced {
		if !readable[name] {
			dead = append(dead, name)
		}
	}
	sort.Strings(dead)

	for _, name := range dead {
		t.Errorf("%s is set by %v but is read by no code path; "+
			"either wire it up or remove it, because it silently does nothing today",
			name, referenced[name])
	}
}

// TestBrokerReadsNoConfigFile guards the claim the documentation now makes.
// If a YAML loader is ever added to the broker, the configuration reference
// stops being true and has to be rewritten, so fail loudly rather than let the
// docs quietly drift back into fiction.
func TestBrokerReadsNoConfigFile(t *testing.T) {
	root := repoRoot(t)

	body, err := os.ReadFile(filepath.Join(root, "cmd", "goqueue", "main.go"))
	if err != nil {
		t.Fatalf("reading broker main: %v", err)
	}
	src := string(body)

	for _, forbidden := range []string{"yaml.Unmarshal", "yaml.NewDecoder", "flag.Parse"} {
		if strings.Contains(src, forbidden) {
			t.Errorf("cmd/goqueue/main.go now calls %s, so the broker parses something "+
				"beyond the environment; update website/docs/configuration/reference.md, "+
				"which states it reads no config file and no flags", forbidden)
		}
	}
}

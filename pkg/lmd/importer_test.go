package lmd

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportPeersFromTarMissingBackends(t *testing.T) {
	// ab12 site does not have backends.json
	archive := createTestImportTar(t,
		[]string{"sites/", "sites/ab12/", "sites/ok/"},
		map[string][]byte{"sites/ok/backends.json": testBackendsJSON(t, "ok", "OkPeer", "127.0.0.1:1")},
	)
	_, err := importPeersFromTar(createTestLMDInstance(), archive)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ab12")
	assert.Contains(t, err.Error(), "backends.json")
}

func testBackendsJSON(t *testing.T, id, name, addr string) []byte {
	t.Helper()

	columns := []any{"addr", "flags", "last_error", "last_online", "last_update", "peer_key", "peer_name", "queries", "response_time", "section", "status"}
	row := []any{addr, []any{}, "", 0, 0, id, name, 0, 0, "", 0}
	data, err := json.Marshal([]any{columns, row})
	require.NoError(t, err)

	return data
}

func createTestImportTar(t *testing.T, dirs []string, files map[string][]byte) string {
	t.Helper()

	archive := t.TempDir() + "/import.tar.gz"
	file, err := os.Create(archive)
	require.NoError(t, err)
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)

	for _, dir := range dirs {
		require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0o755}))
	}
	for name, content := range files {
		require.NoError(t, tarWriter.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}))
		_, err = tarWriter.Write(content)
		require.NoError(t, err)
	}

	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	require.NoError(t, file.Close())

	return archive
}

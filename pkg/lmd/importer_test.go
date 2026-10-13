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
	//nolint:govet // no need to fieldalign test struct
	tests := []struct {
		name    string
		dirs    []string
		files   map[string][]byte
		want    int
		wantErr string
	}{
		{
			name:    "folder without backends.json",
			dirs:    []string{"sites/", "sites/ab12/"},
			wantErr: "ab12",
		},
		{
			name: "folder without backends.json next to valid peer",
			dirs: []string{"sites/", "sites/ab12/", "sites/ok/"},
			files: map[string][]byte{
				"sites/ok/backends.json": testBackendsJSON(t, "ok", "OkPeer", "127.0.0.1:1"),
			},
			wantErr: "ab12",
		},
		{
			name: "valid archive",
			dirs: []string{"sites/", "sites/ok/"},
			files: map[string][]byte{
				"sites/ok/backends.json": testBackendsJSON(t, "ok", "OkPeer", "127.0.0.1:1"),
			},
			want: 1,
		},
	}

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			archive := createTestImportTar(t, testcase.dirs, testcase.files)
			peers, err := importPeersFromTar(createTestLMDInstance(), archive)
			if testcase.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testcase.wantErr)
				assert.Contains(t, err.Error(), "backends.json")

				return
			}
			require.NoError(t, err)
			assert.Len(t, peers, testcase.want)
		})
	}
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

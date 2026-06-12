// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** {@link BoundStorageConformanceTest} over the file-backed cross-process tier. */
class SqliteFileBoundConformanceTest extends BoundStorageConformanceTest {

    @TempDir
    Path tmp;

    @Override
    FunctionStorage createBackend() {
        return new SqliteFunctionStorage(tmp.resolve("bound.db").toString());
    }
}

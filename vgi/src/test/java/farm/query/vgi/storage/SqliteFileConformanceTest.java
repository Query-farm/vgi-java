// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** {@link FunctionStorageConformanceTest} over the file-backed cross-process tier. */
class SqliteFileConformanceTest extends FunctionStorageConformanceTest {

    @TempDir
    Path tmp;

    @Override
    FunctionStorage createStore() {
        return new SqliteFunctionStorage(tmp.resolve("conformance.db").toString());
    }
}

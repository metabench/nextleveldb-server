import { describe, it, expect } from 'vitest';
import xas2 from 'xas2';

import NextLevelDB_Core_Server from '../nextleveldb-core-server';

describe('NextLevelDB_Core_Server basics', () => {
    it('resolves db path and default port from spec', () => {
        const server = new NextLevelDB_Core_Server({ path: '/tmp/db' });
        expect(server.db_path).toBe('/tmp/db');
        expect(server.port).toBe(420);
    });

    it('prefers explicit db_path and port', () => {
        const server = new NextLevelDB_Core_Server({ db_path: '/custom', port: 9999 });
        expect(server.db_path).toBe('/custom');
        expect(server.port).toBe(9999);
    });

    it('exposes levelup key prefix bounds as xas2 buffers', () => {
        const server = new NextLevelDB_Core_Server({ db_path: '/tmp/db' });
        const [min, max] = server.core_lu_buffers;
        expect(min.equals(xas2(0).buffer)).toBe(true);
        expect(max.equals(xas2(9).buffer)).toBe(true);
    });

    it('derives table metadata from an attached model', () => {
        const server = new NextLevelDB_Core_Server({ db_path: '/tmp/db' });
        server.model = {
            tables: [{ id: 2 }, { id: 7 }],
            table_ids_with_indexes: [7, 11]
        };

        expect(server.all_table_ids).toEqual([2, 7]);
        expect(server.table_ids_with_indexes).toEqual([7, 11]);
    });
});

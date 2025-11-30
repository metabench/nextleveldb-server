import { describe, it, expect, vi } from 'vitest';

import NextLevelDB_P2P_Server from '../nextleveldb-p2p-server';
import Model from 'nextleveldb-model';

const Model_Database = Model.Database;

describe('NextLevelDB_P2P_Server helpers', () => {
    it('looks up clients by configured map', () => {
        const server = new NextLevelDB_P2P_Server({ db_path: '/tmp/db', port: 9999 });
        server.clients = ['first', 'second'];
        server.map_client_indexes = { alpha: 0, beta: 1 };

        expect(server.get_client_by_db_name('beta')).toBe('second');
        expect(server.get_client_by_db_name('missing')).toBeUndefined();
    });

    it('diffs local and remote tables through diff_model_rows', async () => {
        const server = new NextLevelDB_P2P_Server({ db_path: '/tmp/db', port: 9999 });
        const localRecords = [{ id: 1 }];
        const remoteRecords = [{ id: 1, remote: true }];

        server.get_table_records = vi.fn((table, decode, remove, callback) => {
            callback(null, localRecords);
        });

        const client = {
            get_table_records: vi.fn((table, decode, remove, callback) => {
                callback(null, remoteRecords);
            })
        };

        server.get_client_by_db_name = vi.fn(() => client);

        const diffSpy = vi.spyOn(Model_Database, 'diff_model_rows').mockReturnValue(['delta']);

        const diff = await new Promise((resolve, reject) => {
            server.diff_local_and_remote_table('remote-db', 'tbl', (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });

        expect(diff).toEqual(['delta']);
        expect(server.get_table_records).toHaveBeenCalledWith('tbl', true, true, expect.any(Function));
        expect(client.get_table_records).toHaveBeenCalledWith('tbl', true, true, expect.any(Function));
        expect(diffSpy).toHaveBeenCalledWith(localRecords, remoteRecords);

        diffSpy.mockRestore();
    });

    it('loads local and remote models via client and own loader', async () => {
        const server = new NextLevelDB_P2P_Server({ db_path: '/tmp/db', port: 9999 });

        const remoteModel = { name: 'remote' };
        const localModel = { name: 'local' };

        const client = {
            load_core: vi.fn(cb => cb(null, remoteModel))
        };
        server.get_client_by_db_name = vi.fn(() => client);
        server.load_model = vi.fn(cb => cb(null, localModel));

        const models = await new Promise((resolve, reject) => {
            server.get_local_and_remote_models('db1', (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });

        expect(models).toEqual([localModel, remoteModel]);
        expect(client.load_core).toHaveBeenCalled();
        expect(server.load_model).toHaveBeenCalled();
    });

    it('surfaces missing client errors from get_local_and_remote_models', async () => {
        const server = new NextLevelDB_P2P_Server({ db_path: '/tmp/db', port: 9999 });
        server.get_client_by_db_name = vi.fn(() => undefined);

        await expect(
            new Promise((resolve, reject) => {
                server.get_local_and_remote_models('absent', (err, result) => {
                    if (err) reject(err);
                    else resolve(result);
                });
            })
        ).rejects.toThrow('No client found for database absent');
    });
});

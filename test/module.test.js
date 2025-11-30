import { describe, it, expect } from 'vitest';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);

const exportsEntry = require('../module');
const NextLevelDB_Server = require('../nextleveldb-server');
const NextLevelDB_Safer_Server = require('../nextleveldb-safer-server');
const NextLevelDB_Core_Server = require('../nextleveldb-core-server');
const NextLevelDB_P2P_Server = require('../nextleveldb-p2p-server');

describe('module exports', () => {
    it('exposes the correct server classes', () => {
        expect(exportsEntry.NextLevelDB_Server).toBe(NextLevelDB_Server);
        expect(exportsEntry.NextLevelDB_Safer_Server).toBe(NextLevelDB_Safer_Server);
        expect(exportsEntry.NextLevelDB_Core_Server).toBe(NextLevelDB_Core_Server);
        expect(exportsEntry.NextLevelDB_P2P_Server).toBe(NextLevelDB_P2P_Server);
    });

    it('keeps safer and standard server exports distinct', () => {
        expect(exportsEntry.NextLevelDB_Server).not.toBe(NextLevelDB_Safer_Server);
    });
});

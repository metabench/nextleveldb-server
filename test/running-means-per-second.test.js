import { describe, it, expect } from 'vitest';

import RunningMeansPerSecond from '../running-means-per-second';

const toArray = typed => Array.from(typed);

describe('Running_Means_Per_Second', () => {
    it('initialises with expected defaults and records current slot before warm-up', () => {
        const tracker = new RunningMeansPerSecond();
        tracker.add(0, 3);
        tracker.increment(1);

        const ticks = [];
        tracker.on('tick', payload => ticks.push(toArray(payload.ta_res)));

        tracker.tick();

        expect(tracker.num_data_points).toBe(4);
        expect(tracker.lookback_duration_s).toBe(10);
        expect(tracker.ta_working.length).toBe(40);
        expect(tracker.ta_res.length).toBe(4);
        expect(toArray(tracker.ta_res)).toEqual([3, 1, 0, 0]);
        expect(ticks).toEqual([[3, 1, 0, 0]]);
        expect(tracker.rotator_position).toBe(1);
    });

    it('computes rolling averages after the warm-up period', () => {
        const tracker = new RunningMeansPerSecond();
        tracker.operating_s = 9; // Next tick enters averaging branch

        // Populate 10-second lookback window.
        for (let i = 0; i < tracker.lookback_duration_s; i++) {
            tracker.ta_working[i] = i + 1; // Data point 0 -> average 5.5
            tracker.ta_working[tracker.lookback_duration_s + i] = 2; // Data point 1 -> average 2
        }

        const ticks = [];
        tracker.on('tick', payload => ticks.push(toArray(payload.ta_res)));

        tracker.tick();

        expect(ticks[0][0]).toBeCloseTo(5.5);
        expect(ticks[0][1]).toBeCloseTo(2);
        expect(ticks[0][2]).toBe(0);
        expect(ticks[0][3]).toBe(0);
        expect(tracker.rotator_position).toBe(1);
        expect(tracker.ta_working[1]).toBe(0);
        expect(tracker.ta_working[tracker.lookback_duration_s + 1]).toBe(0);
    });

    it('wraps the rotator position and clears slots on full rotation', () => {
        const tracker = new RunningMeansPerSecond();
        tracker.operating_s = 9;

        // Preload a value in the last slot before wrap.
        const lastSlotIndex = tracker.lookback_duration_s - 1;
        tracker.ta_working[lastSlotIndex] = 5;

        // Move rotator to last slot.
        tracker.rotator_position = lastSlotIndex;
        tracker.tick(); // Advances to 0 and clears position 0

        expect(tracker.rotator_position).toBe(0);
        expect(tracker.ta_working[lastSlotIndex]).toBe(5);
        expect(tracker.ta_working[0]).toBe(0);
    });
});

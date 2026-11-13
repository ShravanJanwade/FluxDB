import { describe, expect, it } from "vitest";
import { toLine } from "./lineProtocol";

describe("toLine", () => {
  it("writes tags sorted, typed fields and a millisecond timestamp", () => {
    expect(
      toLine({
        measurement: "vitals",
        tags: { source: "monitor", bed: "B3" },
        fields: { hr: 88n, temp_c: 37.2, on_o2: true, acvpu: "A" },
        time: 1731412800000,
      }),
    ).toBe('vitals,bed=B3,source=monitor hr=88i,temp_c=37.2,on_o2=true,acvpu="A" 1731412800000');
  });

  it("escapes separators in names and quotes in strings", () => {
    expect(
      toLine({
        measurement: "power meter",
        tags: { "site name": "North, East", zone: "a=b" },
        fields: { "load kw": 1.5, note: 'say "hi" \\ ok' },
        time: 1,
      }),
    ).toBe('power\\ meter,site\\ name=North\\,\\ East,zone=a\\=b load\\ kw=1.5,note="say \\"hi\\" \\\\ ok" 1');
  });

  it("drops empty tags and missing fields", () => {
    expect(toLine({ measurement: "m", tags: { a: "", b: undefined }, fields: { x: 1, y: null, z: undefined }, time: 5 })).toBe(
      "m x=1 5",
    );
  });

  it("rejects what line protocol cannot carry", () => {
    expect(() => toLine({ measurement: "m", fields: {}, time: 1 })).toThrow(/at least one field/);
    expect(() => toLine({ measurement: "m", fields: { x: Number.NaN }, time: 1 })).toThrow(/finite/);
    expect(() => toLine({ measurement: "a\nb", fields: { x: 1 }, time: 1 })).toThrow(/line break/);
  });
});

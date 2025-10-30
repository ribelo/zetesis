# Chunk Coverage Check — `data/kio/raw/kio/kio_5430.pdf`

- Extracted at: `cargo run --bin zetesis-app -- extract-structured data/kio/raw/kio/kio_5430.pdf`
- Source token count (PDF text via `segment-pdf`): **5 597**
- Chunk token count (sum of `chunks[].body`): **4 907**
- Relative difference: **12.3 %** shorter than the source (within the 15 % guardrail)

## Notable gaps

The largest omissions are at the very beginning of the document. The raw PDF text starts with:

```
Sygn. akt KIO/UZP 113/08 Sygn. akt KIO/UZP 124/08 WYROK z dnia 28 lutego 2008 r.
Krajowa Izba Odwoławcza - w składzie: Przewodniczący: Małgorzata Stręciwilk
Członkowie: Stanisław Sadowy Małgorzata Rakowska Protokolant: Magdalena Pazura ...
```

This header block is absent from the concatenated chunk bodies returned by Gemini, which likely accounts for most of the ~12 % deficit.

Remaining mismatches correspond to smaller formatting differences (multiple spaces, punctuation) but do not materially exceed the guardrail.***

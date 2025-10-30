# OCR Provider Cost Reference

This note captures the pricing mechanics and back-of-the-envelope totals for our
current OCR providers. Numbers come from the sample run on
`data/kio/raw/kio/kio_5430.pdf` (11 pages); adjust if token characteristics change.

## Pricing Cheat Sheet

- **Gemini 2.5 Flash Lite (paid tier)**: $0.10 per 1 M input tokens covering
  text/images/video, $0.40 per 1 M output tokens (reasoning “thinking” tokens
  included). The Batch API halves both rates to $0.05 / $0.20. Free tier exists
  but is rate-limited.
- **Gemini multimodal accounting**: each image tile up to 768×768 px is billed
  as 258 tokens (images ≤384 px on both sides still cost 258 tokens). Video
  inputs consume 263 tokens per second; audio inputs consume 32 tokens per
  second. The usage metadata returned by the Gemini API already reflects these
  conversions—base any cost estimates on those counters.
- **DeepSeek-OCR (DeepInfra)**: $0.03 per 1 M input tokens, $0.10 per 1 M output
  tokens.
- **olmOCR-2-7B-1025 (DeepInfra)**: $0.09 per 1 M input tokens, $0.19 per
  1 M output tokens. (No official reference doc—taken from the model landing
  page.)

## Token Averages From `kio_5430.pdf`

| Provider                | Input tokens / page | Output tokens / page |
|-------------------------|--------------------:|---------------------:|
| Gemini 2.5 Flash Lite   |                274  |               1,123.4 |
| DeepSeek-OCR            |                917.6|               1,442.6 |
| olmOCR-2-7B-1025        |              1,508  |               1,300.5 |

Each figure is the aggregate usage reported by the provider divided by the
11-page document count. They represent an “average” KIO page—documents with
unusual layouts may diverge.

## Estimated Spend for ~500 k KIO Pages

Multiplying the per-page token counts by 500,000 pages yields the following
order-of-magnitude totals:

| Provider                | Cost / page (USD) | 500 k pages (USD) |
|-------------------------|------------------:|------------------:|
| Gemini 2.5 Flash Lite   |        ~$0.00048  |           ~$240   |
| Gemini 2.5 Flash Lite (Batch API) |        ~$0.00024  |           ~$120   |
| DeepSeek-OCR            |        ~$0.00017  |            ~$86   |
| olmOCR-2-7B-1025        |        ~$0.00038  |           ~$191   |

Notes:

- Gemini costs include the implicit per-image tokenization; a separate image
  surcharge isn’t needed when using the official API.
- DeepSeek’s response quality was inconsistent on this sample (page 11 looped).
  Token prices are cheap but factor retry/fallback strategies into the budget.
- olmOCR is accurate but materially more expensive than DeepSeek. Gemini sits
  between them on price, and the Batch API halves the rate if throughput needs
  justify the extra plumbing.

Re-run the measurement periodically—real-world tokens per page vary with
document density, language, and prompt shape.

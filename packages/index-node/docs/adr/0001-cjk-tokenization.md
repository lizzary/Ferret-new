# ADR 0001: Use tantivy-go's bundled Jieba tokenizer for CJK text

- Status: Accepted
- Date: 2026-07-13

## Context

The index must search Chinese text correctly. Tantivy's default/simple tokenizer does not provide useful Chinese word segmentation. The pinned `github.com/anyproto/tantivy-go v1.0.6` API was inspected before integration: it exposes `TokenizerJieba` and `RegisterTextAnalyzerJieba`, as well as n-gram and simple analyzers.

## Decision

Use the binding's bundled Jieba analyzer for `filename`, `path_text`, and `content`. Exact identifiers and filter-like fields continue to use the raw tokenizer. Register Jieba with the same tokenizer name used by the schema before indexing or searching.

## Consequences

- Chinese queries receive dictionary-based segmentation without another CGO dependency.
- Latin words are also accepted by Jieba, so M1 can use one content field for mixed-language documents.
- The native Tantivy library includes the Jieba dictionary, increasing its binary size.
- Tokenization behavior is coupled to the dictionary bundled by the pinned native library; upgrading that library requires search regression tests.
- If a future fork removes Jieba, the documented fallback is a 2-gram analyzer behind this adapter, not a business-layer change.

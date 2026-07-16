package index

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultSearchTopK = 20
	MaxSearchTopK     = 1000

	rrfRankConstant   = 60
	overfetchMultiple = 4
	maxSnippetRunes   = 240

	SourceContent  = "content"
	SourceNote     = "note"
	SourceSemantic = "semantic"
)

var (
	ErrInvalidSearchRequest   = errors.New("index: invalid search request")
	ErrInvalidSearchResponse  = errors.New("index: invalid search response")
	ErrSearchDependencyAbsent = errors.New("index: search dependency is not configured")
)

// Mode selects the ranked retrieval routes. Its zero value is deliberately
// hybrid so callers which omit the field receive the product default.
type Mode uint8

const (
	ModeHybrid Mode = iota
	ModeKeyword
	ModeSemantic
)

func (mode Mode) String() string {
	switch mode {
	case ModeHybrid:
		return "hybrid"
	case ModeKeyword:
		return "keyword"
	case ModeSemantic:
		return "semantic"
	default:
		return "unknown"
	}
}

type Filters struct {
	PathPrefix  string
	Kinds       []store.FileKind
	MTimeFromNS *int64
	MTimeToNS   *int64
}

type SearchRequest struct {
	Query   string
	Mode    Mode
	TopK    int
	Filters Filters
}

type SearchResponse struct {
	Hits             []Hit
	DegradedSemantic bool
	// Incomplete is true when at least one backend filled the maximum bounded
	// candidate window but catalog filtering still left fewer than TopK hits.
	// Callers can distinguish that condition from an authoritative empty result.
	Incomplete bool
}

type Hit struct {
	FileID    int64
	Path      string
	Kind      store.FileKind
	Score     float64
	Sources   []string
	Snippet   string
	FrameTSMS *int64
	Status    store.FileStatus
}

type QueryEmbedding struct {
	Values       []float32
	ModelVersion string
}

// VectorHit is one ranked ANN result. Key is the packed
// (file_id << 16) | frame_idx identifier and is checked against the expanded
// fields before the result is allowed into fusion.
type VectorHit struct {
	Key          uint64
	FileID       int64
	FrameIndex   int
	FrameTSMS    *int64
	Score        float32
	ModelVersion string
}

type KeywordSearcher interface {
	SearchKeyword(context.Context, string, int) ([]KeywordHit, error)
}

type CatalogSource interface {
	GetFilesByIDs(context.Context, []int64) (map[int64]store.File, error)
}

type QueryEmbedder interface {
	EmbedText(context.Context, string) (QueryEmbedding, error)
}

type SemanticSearcher interface {
	Search(context.Context, []float32, string, int) ([]VectorHit, error)
}

type SearchObserver interface {
	ObserveSearch(mode Mode, elapsed time.Duration, degradedSemantic bool)
}

type SearchConfig struct {
	// IsSemanticUnavailable identifies compute/dependency availability failures.
	// It is also consulted for ErrVectorModelMismatch, which can occur briefly
	// during a model switch; all other local ANN failures remain visible.
	IsSemanticUnavailable func(error) bool
	Observer              SearchObserver
}

// SearchService owns query routing and rank fusion. Backends remain interfaces
// so the same service can be wired to gRPC or in-process compute implementations.
type SearchService struct {
	keyword  KeywordSearcher
	catalog  CatalogSource
	embedder QueryEmbedder
	semantic SemanticSearcher
	config   SearchConfig
}

func NewSearchService(
	keyword KeywordSearcher,
	catalog CatalogSource,
	embedder QueryEmbedder,
	semantic SemanticSearcher,
	config SearchConfig,
) (*SearchService, error) {
	if catalog == nil {
		return nil, fmt.Errorf("%w: catalog", ErrSearchDependencyAbsent)
	}
	return &SearchService{
		keyword: keyword, catalog: catalog, embedder: embedder,
		semantic: semantic, config: config,
	}, nil
}

func (service *SearchService) Search(ctx context.Context, request SearchRequest) (response SearchResponse, returnErr error) {
	if service == nil {
		return response, fmt.Errorf("%w: service", ErrSearchDependencyAbsent)
	}
	if ctx == nil {
		return response, fmt.Errorf("%w: context is required", ErrInvalidSearchRequest)
	}
	if err := ctx.Err(); err != nil {
		return response, err
	}

	plan, err := normalizeSearchRequest(request)
	if err != nil {
		return response, err
	}
	if err := service.requireModeDependencies(plan.request.Mode); err != nil {
		return response, err
	}
	started := time.Now()
	if service.config.Observer != nil {
		defer func() {
			service.config.Observer.ObserveSearch(
				plan.request.Mode,
				time.Since(started),
				response.DegradedSemantic,
			)
		}()
	}

	// The backends expose a bounded top-N API rather than offsets. Start with
	// the normal overfetch window, then grow it geometrically only when catalog
	// filtering left too few live results and a backend filled the prior window.
	// This avoids false-empty filtered searches without making every query pull
	// the maximum 1000 candidates.
	limit := candidateLimit(plan.request.TopK)
	keywordActive := usesKeyword(plan.request.Mode)
	semanticActive := usesSemantic(plan.request.Mode)
	needsEmbedding := semanticActive
	var embedding QueryEmbedding
	var keywordHits []KeywordHit
	var semanticHits []VectorHit

	for {
		round, searchErr := service.searchCandidateRound(
			ctx,
			plan.request.Query,
			limit,
			keywordActive,
			semanticActive,
			embedding,
			needsEmbedding,
		)
		if searchErr != nil {
			return response, searchErr
		}
		if err := ctx.Err(); err != nil {
			return response, err
		}

		if keywordActive {
			keywordHits = round.keyword
		}
		if semanticActive {
			if round.degradedSemantic {
				response.DegradedSemantic = true
				semanticHits = nil
			} else {
				embedding = round.embedding
				semanticHits = round.semantic
			}
		}
		needsEmbedding = false

		keywordHasMore := keywordActive && len(round.keyword) == limit
		semanticHasMore := semanticActive && !round.degradedSemantic && len(round.semantic) == limit

		fileIDs := collectCandidateFileIDs(keywordHits, semanticHits)
		if len(fileIDs) == 0 {
			response.Hits = nil
		} else {
			catalog, catalogErr := service.catalog.GetFilesByIDs(ctx, fileIDs)
			if catalogErr != nil {
				return response, fmt.Errorf("index: load search catalog metadata: %w", catalogErr)
			}
			if validationErr := validateCatalogResponse(catalog, fileIDs); validationErr != nil {
				return response, validationErr
			}

			keywordRanked := rankKeywordHits(keywordHits, catalog, plan)
			semanticRanked := rankSemanticHits(semanticHits, catalog, plan, embedding.ModelVersion)
			response.Hits = fuseRankedHits(keywordRanked, semanticRanked, plan.request.TopK)
		}
		if len(response.Hits) >= plan.request.TopK {
			return response, nil
		}
		if limit == MaxSearchTopK {
			response.Incomplete = keywordHasMore || semanticHasMore
			return response, nil
		}

		keywordActive = keywordHasMore
		semanticActive = semanticHasMore
		if !keywordActive && !semanticActive {
			return response, nil
		}
		limit = expandedCandidateLimit(limit)
	}
}

type candidateRound struct {
	keyword          []KeywordHit
	semantic         []VectorHit
	embedding        QueryEmbedding
	degradedSemantic bool
}

func (service *SearchService) searchCandidateRound(
	ctx context.Context,
	query string,
	limit int,
	searchKeyword bool,
	searchSemantic bool,
	embedding QueryEmbedding,
	embedQuery bool,
) (candidateRound, error) {
	var round candidateRound
	group, groupCtx := errgroup.WithContext(ctx)
	if searchKeyword {
		group.Go(func() error {
			hits, err := service.keyword.SearchKeyword(groupCtx, query, limit)
			if err != nil {
				return fmt.Errorf("index: keyword route: %w", err)
			}
			if len(hits) > limit {
				hits = hits[:limit]
			}
			if err := validateKeywordHits(hits); err != nil {
				return err
			}
			round.keyword = hits
			return nil
		})
	}
	if searchSemantic {
		group.Go(func() error {
			queryEmbedding := embedding
			if embedQuery {
				var err error
				queryEmbedding, err = service.embedder.EmbedText(groupCtx, query)
				if err != nil {
					if ctxErr := ctx.Err(); ctxErr != nil {
						return ctxErr
					}
					if service.config.IsSemanticUnavailable != nil && service.config.IsSemanticUnavailable(err) {
						round.degradedSemantic = true
						return nil
					}
					return fmt.Errorf("index: embed semantic query: %w", err)
				}
				if err := validateQueryEmbedding(queryEmbedding); err != nil {
					return err
				}
			}
			hits, err := service.semantic.Search(
				groupCtx,
				queryEmbedding.Values,
				queryEmbedding.ModelVersion,
				limit,
			)
			if err != nil {
				if errors.Is(err, ErrVectorModelMismatch) &&
					service.config.IsSemanticUnavailable != nil &&
					service.config.IsSemanticUnavailable(err) {
					round.degradedSemantic = true
					return nil
				}
				return fmt.Errorf("index: semantic route: %w", err)
			}
			if len(hits) > limit {
				hits = hits[:limit]
			}
			if err := validateVectorHits(hits, queryEmbedding.ModelVersion); err != nil {
				return err
			}
			round.embedding = queryEmbedding
			round.semantic = hits
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return candidateRound{}, err
	}
	return round, nil
}

type normalizedSearchRequest struct {
	request SearchRequest
	kinds   map[store.FileKind]struct{}
}

func normalizeSearchRequest(request SearchRequest) (normalizedSearchRequest, error) {
	request.Query = strings.TrimSpace(request.Query)
	if request.Query == "" {
		return normalizedSearchRequest{}, fmt.Errorf("%w: query is required", ErrInvalidSearchRequest)
	}
	if request.Mode != ModeHybrid && request.Mode != ModeKeyword && request.Mode != ModeSemantic {
		return normalizedSearchRequest{}, fmt.Errorf("%w: unknown mode %d", ErrInvalidSearchRequest, request.Mode)
	}
	if request.TopK == 0 {
		request.TopK = DefaultSearchTopK
	}
	if request.TopK < 1 || request.TopK > MaxSearchTopK {
		return normalizedSearchRequest{}, fmt.Errorf(
			"%w: top_k must be between 1 and %d",
			ErrInvalidSearchRequest,
			MaxSearchTopK,
		)
	}
	if request.Filters.MTimeFromNS != nil && request.Filters.MTimeToNS != nil &&
		*request.Filters.MTimeFromNS > *request.Filters.MTimeToNS {
		return normalizedSearchRequest{}, fmt.Errorf("%w: mtime range is inverted", ErrInvalidSearchRequest)
	}

	kinds := make(map[store.FileKind]struct{}, len(request.Filters.Kinds))
	for _, kind := range request.Filters.Kinds {
		switch kind {
		case store.FileKindText, store.FileKindImage, store.FileKindVideo, store.FileKindOther:
			kinds[kind] = struct{}{}
		default:
			return normalizedSearchRequest{}, fmt.Errorf("%w: unknown file kind %q", ErrInvalidSearchRequest, kind)
		}
	}
	request.Filters.Kinds = append([]store.FileKind(nil), request.Filters.Kinds...)
	return normalizedSearchRequest{request: request, kinds: kinds}, nil
}

func (service *SearchService) requireModeDependencies(mode Mode) error {
	if usesKeyword(mode) && service.keyword == nil {
		return fmt.Errorf("%w: keyword searcher", ErrSearchDependencyAbsent)
	}
	if usesSemantic(mode) && service.embedder == nil {
		return fmt.Errorf("%w: query embedder", ErrSearchDependencyAbsent)
	}
	if usesSemantic(mode) && service.semantic == nil {
		return fmt.Errorf("%w: semantic searcher", ErrSearchDependencyAbsent)
	}
	return nil
}

func usesKeyword(mode Mode) bool { return mode == ModeKeyword || mode == ModeHybrid }

func usesSemantic(mode Mode) bool { return mode == ModeSemantic || mode == ModeHybrid }

func candidateLimit(topK int) int {
	if topK >= MaxSearchTopK/overfetchMultiple {
		return MaxSearchTopK
	}
	return topK * overfetchMultiple
}

func expandedCandidateLimit(current int) int {
	if current >= MaxSearchTopK/2 {
		return MaxSearchTopK
	}
	return current * 2
}

func validateKeywordHits(hits []KeywordHit) error {
	for index, hit := range hits {
		if hit.FileID <= 0 {
			return fmt.Errorf("%w: keyword hit %d has invalid file ID", ErrInvalidSearchResponse, index)
		}
		if math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) {
			return fmt.Errorf("%w: keyword hit %d has non-finite score", ErrInvalidSearchResponse, index)
		}
	}
	return nil
}

func validateQueryEmbedding(embedding QueryEmbedding) error {
	if strings.TrimSpace(embedding.ModelVersion) == "" {
		return fmt.Errorf("%w: query embedding model version is empty", ErrInvalidSearchResponse)
	}
	if len(embedding.Values) == 0 {
		return fmt.Errorf("%w: query embedding has no dimensions", ErrInvalidSearchResponse)
	}
	var normSquared float64
	for _, value := range embedding.Values {
		converted := float64(value)
		if math.IsNaN(converted) || math.IsInf(converted, 0) {
			return fmt.Errorf("%w: query embedding contains a non-finite value", ErrInvalidSearchResponse)
		}
		normSquared += converted * converted
	}
	norm := math.Sqrt(normSquared)
	if math.Abs(norm-1) > 1e-3 {
		return fmt.Errorf("%w: query embedding is not L2 normalized (norm %.6f)", ErrInvalidSearchResponse, norm)
	}
	return nil
}

func validateVectorHits(hits []VectorHit, modelVersion string) error {
	for index, hit := range hits {
		if hit.FileID <= 0 {
			return fmt.Errorf("%w: semantic hit %d has invalid file ID", ErrInvalidSearchResponse, index)
		}
		if hit.FrameIndex < 0 || hit.FrameIndex >= 1<<16 {
			return fmt.Errorf("%w: semantic hit %d has invalid frame index", ErrInvalidSearchResponse, index)
		}
		if uint64(hit.FileID) > math.MaxUint64>>16 {
			return fmt.Errorf("%w: semantic hit %d file ID cannot be packed", ErrInvalidSearchResponse, index)
		}
		expectedKey := uint64(hit.FileID)<<16 | uint64(hit.FrameIndex)
		if hit.Key != expectedKey {
			return fmt.Errorf("%w: semantic hit %d has mismatched key", ErrInvalidSearchResponse, index)
		}
		if hit.FrameTSMS != nil && *hit.FrameTSMS < 0 {
			return fmt.Errorf("%w: semantic hit %d has negative frame timestamp", ErrInvalidSearchResponse, index)
		}
		if math.IsNaN(float64(hit.Score)) || math.IsInf(float64(hit.Score), 0) {
			return fmt.Errorf("%w: semantic hit %d has non-finite score", ErrInvalidSearchResponse, index)
		}
		if hit.ModelVersion != modelVersion {
			return fmt.Errorf(
				"%w: semantic hit %d model %q does not match query model %q",
				ErrInvalidSearchResponse,
				index,
				hit.ModelVersion,
				modelVersion,
			)
		}
	}
	return nil
}

func collectCandidateFileIDs(keyword []KeywordHit, semantic []VectorHit) []int64 {
	seen := make(map[int64]struct{}, len(keyword)+len(semantic))
	for _, hit := range keyword {
		seen[hit.FileID] = struct{}{}
	}
	for _, hit := range semantic {
		seen[hit.FileID] = struct{}{}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	return ids
}

func validateCatalogResponse(catalog map[int64]store.File, requested []int64) error {
	requestedSet := make(map[int64]struct{}, len(requested))
	for _, id := range requested {
		requestedSet[id] = struct{}{}
	}
	for id, file := range catalog {
		if _, ok := requestedSet[id]; !ok {
			continue
		}
		if id <= 0 || file.ID != id {
			return fmt.Errorf("%w: catalog entry %d has file ID %d", ErrInvalidSearchResponse, id, file.ID)
		}
	}
	return nil
}

type rankedHit struct {
	file      store.File
	rank      int
	source    string
	snippet   string
	frameTSMS *int64
}

func rankKeywordHits(hits []KeywordHit, catalog map[int64]store.File, plan normalizedSearchRequest) []rankedHit {
	seen := make(map[int64]struct{}, len(hits))
	ranked := make([]rankedHit, 0, min(len(hits), plan.request.TopK))
	for _, hit := range hits {
		if _, duplicate := seen[hit.FileID]; duplicate {
			continue
		}
		seen[hit.FileID] = struct{}{}
		file, ok := catalog[hit.FileID]
		if !ok || !matchesKeywordCatalog(file, plan) {
			continue
		}
		contentSnippet := ""
		if file.Status == store.FileStatusIndexed {
			contentSnippet = snippet(hit.Content)
		}
		ranked = append(ranked, rankedHit{
			file: file, rank: len(ranked) + 1,
			source: SourceContent, snippet: contentSnippet,
		})
	}
	return ranked
}

func rankSemanticHits(
	hits []VectorHit,
	catalog map[int64]store.File,
	plan normalizedSearchRequest,
	modelVersion string,
) []rankedHit {
	seen := make(map[int64]struct{}, len(hits))
	ranked := make([]rankedHit, 0, min(len(hits), plan.request.TopK))
	for _, hit := range hits {
		if _, duplicate := seen[hit.FileID]; duplicate {
			continue
		}
		seen[hit.FileID] = struct{}{}
		file, ok := catalog[hit.FileID]
		if !ok || file.Status != store.FileStatusIndexed || !matchesRequestFilters(file, plan) ||
			file.EmbedModelVersion == nil || *file.EmbedModelVersion != modelVersion {
			continue
		}
		ranked = append(ranked, rankedHit{
			file: file, rank: len(ranked) + 1,
			source: SourceSemantic, frameTSMS: copyInt64(hit.FrameTSMS),
		})
	}
	return ranked
}

func matchesKeywordCatalog(file store.File, plan normalizedSearchRequest) bool {
	if file.Status != store.FileStatusIndexed && file.Status != store.FileStatusFailed {
		return false
	}
	return matchesRequestFilters(file, plan)
}

func matchesRequestFilters(file store.File, plan normalizedSearchRequest) bool {
	if !pathMatchesPrefix(file.Path, plan.request.Filters.PathPrefix) {
		return false
	}
	if len(plan.kinds) != 0 {
		if _, ok := plan.kinds[file.Kind]; !ok {
			return false
		}
	}
	if from := plan.request.Filters.MTimeFromNS; from != nil && file.MTimeNS < *from {
		return false
	}
	if to := plan.request.Filters.MTimeToNS; to != nil && file.MTimeNS > *to {
		return false
	}
	return true
}

func pathMatchesPrefix(path, prefix string) bool {
	if prefix == "" {
		return true
	}
	path = filepath.Clean(path)
	prefix = filepath.Clean(prefix)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
		prefix = strings.ToLower(prefix)
	}
	if path == prefix {
		return true
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if isSearchPathSeparator(prefix[len(prefix)-1]) {
		return true
	}
	return len(path) > len(prefix) && isSearchPathSeparator(path[len(prefix)])
}

func isSearchPathSeparator(value byte) bool { return value == '/' || value == '\\' }

func snippet(content string) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}
	runes := []rune(content)
	if len(runes) <= maxSnippetRunes {
		return content
	}
	return string(runes[:maxSnippetRunes])
}

func copyInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

type fusedHit struct {
	hit       Hit
	bestRank  int
	sourceSet map[string]struct{}
}

func fuseRankedHits(keyword, semantic []rankedHit, topK int) []Hit {
	fused := make(map[int64]*fusedHit, len(keyword)+len(semantic))
	add := func(candidate rankedHit) {
		item := fused[candidate.file.ID]
		if item == nil {
			item = &fusedHit{
				hit: Hit{
					FileID: candidate.file.ID, Path: candidate.file.Path,
					Kind: candidate.file.Kind, Status: candidate.file.Status,
				},
				bestRank:  candidate.rank,
				sourceSet: make(map[string]struct{}, 2),
			}
			fused[candidate.file.ID] = item
		}
		item.hit.Score += 1 / (float64(rrfRankConstant) + float64(candidate.rank))
		if candidate.rank < item.bestRank {
			item.bestRank = candidate.rank
		}
		item.sourceSet[candidate.source] = struct{}{}
		if candidate.source == SourceContent && item.hit.Snippet == "" {
			item.hit.Snippet = candidate.snippet
		}
		if candidate.source == SourceSemantic {
			item.hit.FrameTSMS = copyInt64(candidate.frameTSMS)
		}
	}
	for _, candidate := range keyword {
		add(candidate)
	}
	for _, candidate := range semantic {
		add(candidate)
	}

	items := make([]*fusedHit, 0, len(fused))
	for _, item := range fused {
		for _, source := range []string{SourceContent, SourceNote, SourceSemantic} {
			if _, ok := item.sourceSet[source]; ok {
				item.hit.Sources = append(item.hit.Sources, source)
			}
		}
		items = append(items, item)
	}
	sort.Slice(items, func(left, right int) bool {
		if items[left].hit.Score != items[right].hit.Score {
			return items[left].hit.Score > items[right].hit.Score
		}
		if items[left].bestRank != items[right].bestRank {
			return items[left].bestRank < items[right].bestRank
		}
		return items[left].hit.FileID < items[right].hit.FileID
	})
	if len(items) > topK {
		items = items[:topK]
	}
	result := make([]Hit, len(items))
	for index, item := range items {
		result[index] = item.hit
	}
	return result
}

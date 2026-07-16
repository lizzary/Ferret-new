package index

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/store"
)

func TestSearchHybridRRFExactDedupAndBestFrame(t *testing.T) {
	t.Parallel()

	frameOne := int64(100)
	frameTwoBest := int64(200)
	frameTwoWorse := int64(300)
	keyword := &fakeKeywordSearcher{hits: []KeywordHit{
		{FileID: 1, Content: "  alpha snippet  ", Score: 9},
		{FileID: 1, Content: "duplicate must not change rank", Score: 8},
		{FileID: 2, Content: "beta snippet", Score: 7},
	}}
	embedder := &fakeQueryEmbedder{embedding: normalizedEmbedding("model-v1")}
	semantic := &fakeSemanticSearcher{hits: []VectorHit{
		vectorHit(2, 0, &frameTwoBest, 0.99, "model-v1"),
		vectorHit(2, 1, &frameTwoWorse, 0.80, "model-v1"),
		vectorHit(1, 0, &frameOne, 0.70, "model-v1"),
	}}
	catalog := &fakeCatalogSource{files: map[int64]store.File{
		1: indexedFile(1, `/root/one.txt`, store.FileKindText, 1, "model-v1"),
		2: indexedFile(2, `/root/two.png`, store.FileKindImage, 2, "model-v1"),
	}}
	service := mustSearchService(t, keyword, catalog, embedder, semantic, SearchConfig{})

	response, err := service.Search(context.Background(), SearchRequest{Query: "ferret", TopK: 2})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if response.DegradedSemantic {
		t.Fatal("hybrid search unexpectedly degraded")
	}
	if got, want := len(response.Hits), 2; got != want {
		t.Fatalf("hit count = %d, want %d", got, want)
	}

	// Both files receive ranks 1 and 2 across the two routes. Their RRF scores
	// and best ranks are equal, so the final deterministic tie-break is file ID.
	wantScore := 1.0/61.0 + 1.0/62.0
	for index, fileID := range []int64{1, 2} {
		hit := response.Hits[index]
		if hit.FileID != fileID {
			t.Fatalf("hit[%d].FileID = %d, want %d", index, hit.FileID, fileID)
		}
		if math.Abs(hit.Score-wantScore) > 1e-15 {
			t.Fatalf("hit[%d].Score = %.17f, want %.17f", index, hit.Score, wantScore)
		}
		if !reflect.DeepEqual(hit.Sources, []string{SourceContent, SourceSemantic}) {
			t.Fatalf("hit[%d].Sources = %#v", index, hit.Sources)
		}
		if hit.Status != store.FileStatusIndexed {
			t.Fatalf("hit[%d].Status = %q", index, hit.Status)
		}
	}
	if response.Hits[0].Snippet != "alpha snippet" || response.Hits[1].Snippet != "beta snippet" {
		t.Fatalf("snippets = %q, %q", response.Hits[0].Snippet, response.Hits[1].Snippet)
	}
	if response.Hits[0].FrameTSMS == nil || *response.Hits[0].FrameTSMS != frameOne {
		t.Fatalf("file 1 frame timestamp = %v", response.Hits[0].FrameTSMS)
	}
	if response.Hits[1].FrameTSMS == nil || *response.Hits[1].FrameTSMS != frameTwoBest {
		t.Fatalf("file 2 did not preserve best frame: %v", response.Hits[1].FrameTSMS)
	}
	if got, want := keyword.limit, 8; got != want {
		t.Fatalf("keyword overfetch = %d, want %d", got, want)
	}
	if got, want := semantic.limit, 8; got != want {
		t.Fatalf("semantic overfetch = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(catalog.ids, []int64{1, 2}) {
		t.Fatalf("catalog IDs = %#v, want sorted unique IDs", catalog.ids)
	}
}

func TestSearchSemanticCatalogAndRequestFilters(t *testing.T) {
	t.Parallel()

	from, to := int64(10), int64(20)
	catalog := &fakeCatalogSource{files: map[int64]store.File{
		1: indexedFile(1, `/root/ok.jpg`, store.FileKindImage, 10, "model-v1"),
		2: withStatus(indexedFile(2, `/root/pending.jpg`, store.FileKindImage, 10, "model-v1"), store.FileStatusPending),
		3: indexedFile(3, `/root/old-model.jpg`, store.FileKindImage, 10, "model-v2"),
		4: indexedFile(4, `/root/text.txt`, store.FileKindText, 10, "model-v1"),
		5: indexedFile(5, `/elsewhere/out.jpg`, store.FileKindImage, 10, "model-v1"),
		6: indexedFile(6, `/rooted/not-a-child.jpg`, store.FileKindImage, 10, "model-v1"),
		7: indexedFile(7, `/root/too-new.jpg`, store.FileKindImage, 21, "model-v1"),
	}}
	semantic := &fakeSemanticSearcher{}
	for id := int64(1); id <= 8; id++ {
		semantic.hits = append(semantic.hits, vectorHit(id, 0, nil, float32(10-id), "model-v1"))
	}
	service := mustSearchService(
		t,
		nil,
		catalog,
		&fakeQueryEmbedder{embedding: normalizedEmbedding("model-v1")},
		semantic,
		SearchConfig{},
	)

	response, err := service.Search(context.Background(), SearchRequest{
		Query: "photo",
		Mode:  ModeSemantic,
		TopK:  20,
		Filters: Filters{
			PathPrefix:  `/root`,
			Kinds:       []store.FileKind{store.FileKindImage, store.FileKindImage},
			MTimeFromNS: &from,
			MTimeToNS:   &to,
		},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if got, want := len(response.Hits), 1; got != want {
		t.Fatalf("hit count = %d, want %d: %#v", got, want, response.Hits)
	}
	if hit := response.Hits[0]; hit.FileID != 1 || !reflect.DeepEqual(hit.Sources, []string{SourceSemantic}) {
		t.Fatalf("unexpected surviving hit: %#v", hit)
	}
}

func TestSearchKeywordDoesNotRequireOrApplyEmbeddingModel(t *testing.T) {
	t.Parallel()

	file := indexedFile(1, `/root/plain.txt`, store.FileKindText, 1, "old-model")
	service := mustSearchService(
		t,
		&fakeKeywordSearcher{hits: []KeywordHit{{FileID: 1, Content: "plain", Score: 1}}},
		&fakeCatalogSource{files: map[int64]store.File{1: file}},
		nil,
		nil,
		SearchConfig{},
	)
	response, err := service.Search(context.Background(), SearchRequest{Query: "plain", Mode: ModeKeyword, TopK: 1})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(response.Hits) != 1 || response.Hits[0].FileID != 1 {
		t.Fatalf("keyword-only hit = %#v", response.Hits)
	}
}

func TestSearchFailedCatalogFileKeepsFilenameRouteWithoutStaleContentOrSemantic(t *testing.T) {
	t.Parallel()

	failed := withStatus(
		indexedFile(1, `/root/failed-report.txt`, store.FileKindText, 1, "model-v1"),
		store.FileStatusFailed,
	)
	keyword := &fakeKeywordSearcher{hits: []KeywordHit{{
		FileID: 1,
		// A stale backend response must not leak body text once SQLite says the
		// current generation failed. The M4 projection normally clears this,
		// but the catalog remains the authority at query time.
		Content: "secret body from an older successful generation",
		Score:   1,
	}}}
	semantic := &fakeSemanticSearcher{hits: []VectorHit{
		vectorHit(1, 0, nil, 1, "model-v1"),
	}}
	service := mustSearchService(
		t,
		keyword,
		&fakeCatalogSource{files: map[int64]store.File{1: failed}},
		&fakeQueryEmbedder{embedding: normalizedEmbedding("model-v1")},
		semantic,
		SearchConfig{},
	)

	response, err := service.Search(context.Background(), SearchRequest{
		Query: "failed-report.txt", Mode: ModeHybrid, TopK: 1,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(response.Hits) != 1 {
		t.Fatalf("hits = %#v, want the failed filename/path keyword hit", response.Hits)
	}
	hit := response.Hits[0]
	if hit.FileID != 1 || hit.Status != store.FileStatusFailed {
		t.Fatalf("failed hit = %#v", hit)
	}
	if hit.Snippet != "" || hit.FrameTSMS != nil {
		t.Fatalf("failed hit leaked stale content or semantic frame: %#v", hit)
	}
	if !reflect.DeepEqual(hit.Sources, []string{SourceContent}) {
		t.Fatalf("failed hit sources = %#v, want keyword only", hit.Sources)
	}
	if want := 1.0 / 61.0; math.Abs(hit.Score-want) > 1e-15 {
		t.Fatalf("failed hit score = %.17f, want keyword-only %.17f", hit.Score, want)
	}
}

func TestSearchExpandsPastInitialWindowForFilteredRankEightyOne(t *testing.T) {
	t.Parallel()

	keyword := &fakeKeywordSearcher{}
	catalog := &fakeCatalogSource{files: make(map[int64]store.File, 81)}
	for id := int64(1); id <= 81; id++ {
		path := fmt.Sprintf(`/other/%03d.txt`, id)
		if id == 81 {
			path = `/wanted/rank-081.txt`
		}
		keyword.hits = append(keyword.hits, KeywordHit{FileID: id, Content: "match", Score: float64(100 - id)})
		catalog.files[id] = indexedFile(id, path, store.FileKindText, id, "model-v1")
	}
	service := mustSearchService(t, keyword, catalog, nil, nil, SearchConfig{})

	response, err := service.Search(context.Background(), SearchRequest{
		Query: "match", Mode: ModeKeyword, TopK: DefaultSearchTopK,
		Filters: Filters{PathPrefix: `/wanted`},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(response.Hits) != 1 || response.Hits[0].FileID != 81 {
		t.Fatalf("filtered hits = %#v, want rank 81", response.Hits)
	}
	if response.Incomplete {
		t.Fatal("rank 81 response is marked incomplete after the backend reported exhaustion")
	}
	if !reflect.DeepEqual(keyword.limits, []int{80, 160}) {
		t.Fatalf("keyword limits = %#v, want bounded expansion from 80 to 160", keyword.limits)
	}
}

func TestSearchMarksFilteredResponseIncompleteAtCandidateHardLimit(t *testing.T) {
	t.Parallel()

	keyword := &fakeKeywordSearcher{}
	catalog := &fakeCatalogSource{files: make(map[int64]store.File, MaxSearchTopK)}
	for id := int64(1); id <= MaxSearchTopK; id++ {
		keyword.hits = append(keyword.hits, KeywordHit{FileID: id, Content: "match", Score: float64(MaxSearchTopK - id)})
		catalog.files[id] = indexedFile(
			id,
			fmt.Sprintf(`/outside/%04d.txt`, id),
			store.FileKindText,
			id,
			"model-v1",
		)
	}
	service := mustSearchService(t, keyword, catalog, nil, nil, SearchConfig{})

	response, err := service.Search(context.Background(), SearchRequest{
		Query: "match", Mode: ModeKeyword, TopK: DefaultSearchTopK,
		Filters: Filters{PathPrefix: `/wanted`},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(response.Hits) != 0 || !response.Incomplete {
		t.Fatalf("hard-limit response = %#v, want transparent incomplete empty result", response)
	}
	wantLimits := []int{80, 160, 320, 640, 1000}
	if !reflect.DeepEqual(keyword.limits, wantLimits) {
		t.Fatalf("keyword limits = %#v, want %#v", keyword.limits, wantLimits)
	}
}

func TestSearchRoutesByMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       Mode
		wantKW     int
		wantEmbed  int
		wantVector int
	}{
		{name: "zero value hybrid", mode: ModeHybrid, wantKW: 1, wantEmbed: 1, wantVector: 1},
		{name: "keyword", mode: ModeKeyword, wantKW: 1},
		{name: "semantic", mode: ModeSemantic, wantEmbed: 1, wantVector: 1},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			keyword := &fakeKeywordSearcher{hits: []KeywordHit{{FileID: 1, Content: "hit", Score: 1}}}
			embedder := &fakeQueryEmbedder{embedding: normalizedEmbedding("model-v1")}
			semantic := &fakeSemanticSearcher{hits: []VectorHit{vectorHit(1, 0, nil, 1, "model-v1")}}
			catalog := &fakeCatalogSource{files: map[int64]store.File{
				1: indexedFile(1, `/root/hit.txt`, store.FileKindText, 1, "model-v1"),
			}}
			service := mustSearchService(t, keyword, catalog, embedder, semantic, SearchConfig{})

			if _, err := service.Search(context.Background(), SearchRequest{Query: "hit", Mode: test.mode, TopK: 1}); err != nil {
				t.Fatalf("Search: %v", err)
			}
			if keyword.calls != test.wantKW || embedder.calls != test.wantEmbed || semantic.calls != test.wantVector {
				t.Fatalf(
					"route calls = keyword:%d embed:%d vector:%d; want %d/%d/%d",
					keyword.calls,
					embedder.calls,
					semantic.calls,
					test.wantKW,
					test.wantEmbed,
					test.wantVector,
				)
			}
			if test.wantVector != 0 && (semantic.modelVersion != "model-v1" || !reflect.DeepEqual(semantic.vector, []float32{1, 0})) {
				t.Fatalf("semantic query = %#v, model %q", semantic.vector, semantic.modelVersion)
			}
		})
	}
}

func TestSearchComputeUnavailableDegradesSemanticRoutes(t *testing.T) {
	t.Parallel()

	unavailable := errors.New("compute unavailable")
	classifier := func(err error) bool { return errors.Is(err, unavailable) }
	tests := []struct {
		name         string
		mode         Mode
		keywordHits  []KeywordHit
		wantHitCount int
	}{
		{
			name: "hybrid keeps keyword results", mode: ModeHybrid,
			keywordHits: []KeywordHit{{FileID: 1, Content: "fallback", Score: 1}}, wantHitCount: 1,
		},
		{name: "semantic succeeds empty", mode: ModeSemantic, wantHitCount: 0},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			observer := &fakeSearchObserver{}
			catalog := &fakeCatalogSource{files: map[int64]store.File{
				1: indexedFile(1, `/root/fallback.txt`, store.FileKindText, 1, "model-v1"),
			}}
			keyword := &fakeKeywordSearcher{hits: test.keywordHits}
			semantic := &fakeSemanticSearcher{}
			service := mustSearchService(
				t,
				keyword,
				catalog,
				&fakeQueryEmbedder{err: unavailable},
				semantic,
				SearchConfig{IsSemanticUnavailable: classifier, Observer: observer},
			)

			response, err := service.Search(context.Background(), SearchRequest{Query: "fallback", Mode: test.mode, TopK: 5})
			if err != nil {
				t.Fatalf("Search: %v", err)
			}
			if !response.DegradedSemantic {
				t.Fatal("DegradedSemantic = false")
			}
			if len(response.Hits) != test.wantHitCount {
				t.Fatalf("hit count = %d, want %d", len(response.Hits), test.wantHitCount)
			}
			if semantic.calls != 0 {
				t.Fatalf("semantic backend called %d times after embed failure", semantic.calls)
			}
			if observer.calls != 1 || observer.mode != test.mode || !observer.degraded {
				t.Fatalf("observer = %#v", observer)
			}
		})
	}
}

func TestSearchSemanticBackendFailureIsNotDegraded(t *testing.T) {
	t.Parallel()

	annErr := errors.New("ann corrupt")
	service := mustSearchService(
		t,
		nil,
		&fakeCatalogSource{},
		&fakeQueryEmbedder{embedding: normalizedEmbedding("model-v1")},
		&fakeSemanticSearcher{err: annErr},
		SearchConfig{IsSemanticUnavailable: func(error) bool { return true }},
	)
	response, err := service.Search(context.Background(), SearchRequest{Query: "x", Mode: ModeSemantic})
	if !errors.Is(err, annErr) {
		t.Fatalf("error = %v, want ANN error", err)
	}
	if response.DegradedSemantic {
		t.Fatal("local ANN error was incorrectly degraded")
	}
}

func TestSearchVectorModelMismatchDegradesOnlyWhenClassified(t *testing.T) {
	t.Parallel()

	modelErr := fmt.Errorf("model switched during query: %w", ErrVectorModelMismatch)
	newService := func(classifier func(error) bool) *SearchService {
		return mustSearchService(
			t,
			&fakeKeywordSearcher{hits: []KeywordHit{{FileID: 1, Content: "fallback", Score: 1}}},
			&fakeCatalogSource{files: map[int64]store.File{
				1: indexedFile(1, `/root/fallback.txt`, store.FileKindText, 1, "model-v1"),
			}},
			&fakeQueryEmbedder{embedding: normalizedEmbedding("model-v1")},
			&fakeSemanticSearcher{err: modelErr},
			SearchConfig{IsSemanticUnavailable: classifier},
		)
	}

	classified := newService(func(err error) bool { return errors.Is(err, ErrVectorModelMismatch) })
	response, err := classified.Search(context.Background(), SearchRequest{
		Query: "fallback", Mode: ModeHybrid, TopK: 1,
	})
	if err != nil {
		t.Fatalf("classified Search: %v", err)
	}
	if !response.DegradedSemantic || len(response.Hits) != 1 || response.Hits[0].FileID != 1 {
		t.Fatalf("classified model mismatch response = %#v", response)
	}

	unclassified := newService(func(error) bool { return false })
	response, err = unclassified.Search(context.Background(), SearchRequest{
		Query: "fallback", Mode: ModeHybrid, TopK: 1,
	})
	if !errors.Is(err, ErrVectorModelMismatch) {
		t.Fatalf("unclassified error = %v, want ErrVectorModelMismatch", err)
	}
	if response.DegradedSemantic {
		t.Fatal("unclassified model mismatch was degraded")
	}
}

func TestSearchRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	from, to := int64(2), int64(1)
	tests := []SearchRequest{
		{},
		{Query: "x", Mode: Mode(99)},
		{Query: "x", TopK: -1},
		{Query: "x", TopK: MaxSearchTopK + 1},
		{Query: "x", Filters: Filters{MTimeFromNS: &from, MTimeToNS: &to}},
		{Query: "x", Filters: Filters{Kinds: []store.FileKind{"archive"}}},
	}
	service := mustSearchService(t, nil, &fakeCatalogSource{}, nil, nil, SearchConfig{})
	for index, request := range tests {
		if _, err := service.Search(context.Background(), request); !errors.Is(err, ErrInvalidSearchRequest) {
			t.Errorf("case %d error = %v, want ErrInvalidSearchRequest", index, err)
		}
	}
	if _, err := service.Search(nil, SearchRequest{Query: "x"}); !errors.Is(err, ErrInvalidSearchRequest) {
		t.Fatalf("nil context error = %v", err)
	}
}

func TestSearchRejectsInvalidBackendResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      Mode
		keyword   []KeywordHit
		embedding QueryEmbedding
		semantic  []VectorHit
	}{
		{name: "keyword file ID", mode: ModeKeyword, keyword: []KeywordHit{{FileID: 0, Score: 1}}},
		{name: "keyword score", mode: ModeKeyword, keyword: []KeywordHit{{FileID: 1, Score: math.NaN()}}},
		{name: "empty embedding", mode: ModeSemantic, embedding: QueryEmbedding{ModelVersion: "model-v1"}},
		{name: "empty model", mode: ModeSemantic, embedding: QueryEmbedding{Values: []float32{1}}},
		{name: "non finite embedding", mode: ModeSemantic, embedding: QueryEmbedding{Values: []float32{float32(math.Inf(1))}, ModelVersion: "model-v1"}},
		{name: "unnormalized embedding", mode: ModeSemantic, embedding: QueryEmbedding{Values: []float32{2}, ModelVersion: "model-v1"}},
		{
			name: "mismatched vector key", mode: ModeSemantic, embedding: normalizedEmbedding("model-v1"),
			semantic: []VectorHit{{Key: 99, FileID: 1, FrameIndex: 0, Score: 1, ModelVersion: "model-v1"}},
		},
		{
			name: "mismatched vector model", mode: ModeSemantic, embedding: normalizedEmbedding("model-v1"),
			semantic: []VectorHit{vectorHit(1, 0, nil, 1, "model-v2")},
		},
		{
			name: "non finite vector score", mode: ModeSemantic, embedding: normalizedEmbedding("model-v1"),
			semantic: []VectorHit{vectorHit(1, 0, nil, float32(math.Inf(1)), "model-v1")},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			service := mustSearchService(
				t,
				&fakeKeywordSearcher{hits: test.keyword},
				&fakeCatalogSource{},
				&fakeQueryEmbedder{embedding: test.embedding},
				&fakeSemanticSearcher{hits: test.semantic},
				SearchConfig{},
			)
			if _, err := service.Search(context.Background(), SearchRequest{Query: "x", Mode: test.mode}); !errors.Is(err, ErrInvalidSearchResponse) {
				t.Fatalf("error = %v, want ErrInvalidSearchResponse", err)
			}
		})
	}
}

func TestSearchCatalogErrorsAndInvalidEntriesAreReturned(t *testing.T) {
	t.Parallel()

	t.Run("store error", func(t *testing.T) {
		storeErr := errors.New("sqlite read failed")
		service := mustSearchService(
			t,
			&fakeKeywordSearcher{hits: []KeywordHit{{FileID: 1, Score: 1}}},
			&fakeCatalogSource{err: storeErr},
			nil,
			nil,
			SearchConfig{},
		)
		if _, err := service.Search(context.Background(), SearchRequest{Query: "x", Mode: ModeKeyword}); !errors.Is(err, storeErr) {
			t.Fatalf("error = %v, want catalog error", err)
		}
	})

	t.Run("entry ID mismatch", func(t *testing.T) {
		service := mustSearchService(
			t,
			&fakeKeywordSearcher{hits: []KeywordHit{{FileID: 1, Score: 1}}},
			&fakeCatalogSource{files: map[int64]store.File{1: {ID: 2}}},
			nil,
			nil,
			SearchConfig{},
		)
		if _, err := service.Search(context.Background(), SearchRequest{Query: "x", Mode: ModeKeyword}); !errors.Is(err, ErrInvalidSearchResponse) {
			t.Fatalf("error = %v, want invalid response", err)
		}
	})
}

func TestSearchRoutesStartConcurrently(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	keywordStarted := make(chan struct{})
	embedStarted := make(chan struct{})
	keyword := &fakeKeywordSearcher{started: keywordStarted, release: release}
	embedder := &fakeQueryEmbedder{
		embedding: normalizedEmbedding("model-v1"), started: embedStarted, release: release,
	}
	service := mustSearchService(
		t,
		keyword,
		&fakeCatalogSource{},
		embedder,
		&fakeSemanticSearcher{},
		SearchConfig{},
	)
	done := make(chan error, 1)
	go func() {
		_, err := service.Search(context.Background(), SearchRequest{Query: "x", Mode: ModeHybrid})
		done <- err
	}()

	waitStarted(t, keywordStarted, "keyword")
	waitStarted(t, embedStarted, "semantic embed")
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Search did not finish after releasing both routes")
	}
}

func TestCandidateLimitIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		topK int
		want int
	}{
		{topK: 1, want: 4},
		{topK: DefaultSearchTopK, want: 80},
		{topK: 249, want: 996},
		{topK: 250, want: 1000},
		{topK: 999, want: 1000},
		{topK: 1000, want: 1000},
	}
	for _, test := range tests {
		if got := candidateLimit(test.topK); got != test.want {
			t.Errorf("candidateLimit(%d) = %d, want %d", test.topK, got, test.want)
		}
	}
	for _, test := range []struct {
		current int
		want    int
	}{
		{current: 4, want: 8},
		{current: 80, want: 160},
		{current: 996, want: 1000},
		{current: 1000, want: 1000},
	} {
		if got := expandedCandidateLimit(test.current); got != test.want {
			t.Errorf("expandedCandidateLimit(%d) = %d, want %d", test.current, got, test.want)
		}
	}
}

type fakeKeywordSearcher struct {
	hits    []KeywordHit
	err     error
	calls   int
	query   string
	limit   int
	limits  []int
	started chan<- struct{}
	release <-chan struct{}
}

func (searcher *fakeKeywordSearcher) SearchKeyword(ctx context.Context, query string, limit int) ([]KeywordHit, error) {
	searcher.calls++
	searcher.query = query
	searcher.limit = limit
	searcher.limits = append(searcher.limits, limit)
	if searcher.started != nil {
		searcher.started <- struct{}{}
	}
	if searcher.release != nil {
		select {
		case <-searcher.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return append([]KeywordHit(nil), searcher.hits...), searcher.err
}

type fakeCatalogSource struct {
	files map[int64]store.File
	err   error
	calls int
	ids   []int64
}

func (catalog *fakeCatalogSource) GetFilesByIDs(_ context.Context, ids []int64) (map[int64]store.File, error) {
	catalog.calls++
	catalog.ids = append([]int64(nil), ids...)
	return catalog.files, catalog.err
}

type fakeQueryEmbedder struct {
	embedding QueryEmbedding
	err       error
	calls     int
	query     string
	started   chan<- struct{}
	release   <-chan struct{}
}

func (embedder *fakeQueryEmbedder) EmbedText(ctx context.Context, query string) (QueryEmbedding, error) {
	embedder.calls++
	embedder.query = query
	if embedder.started != nil {
		embedder.started <- struct{}{}
	}
	if embedder.release != nil {
		select {
		case <-embedder.release:
		case <-ctx.Done():
			return QueryEmbedding{}, ctx.Err()
		}
	}
	return embedder.embedding, embedder.err
}

type fakeSemanticSearcher struct {
	hits         []VectorHit
	err          error
	calls        int
	vector       []float32
	modelVersion string
	limit        int
}

func (searcher *fakeSemanticSearcher) Search(
	_ context.Context,
	vector []float32,
	modelVersion string,
	limit int,
) ([]VectorHit, error) {
	searcher.calls++
	searcher.vector = append([]float32(nil), vector...)
	searcher.modelVersion = modelVersion
	searcher.limit = limit
	return append([]VectorHit(nil), searcher.hits...), searcher.err
}

type fakeSearchObserver struct {
	calls    int
	mode     Mode
	elapsed  time.Duration
	degraded bool
}

func (observer *fakeSearchObserver) ObserveSearch(mode Mode, elapsed time.Duration, degraded bool) {
	observer.calls++
	observer.mode = mode
	observer.elapsed = elapsed
	observer.degraded = degraded
}

func mustSearchService(
	t *testing.T,
	keyword KeywordSearcher,
	catalog CatalogSource,
	embedder QueryEmbedder,
	semantic SemanticSearcher,
	config SearchConfig,
) *SearchService {
	t.Helper()
	service, err := NewSearchService(keyword, catalog, embedder, semantic, config)
	if err != nil {
		t.Fatalf("NewSearchService: %v", err)
	}
	return service
}

func indexedFile(id int64, path string, kind store.FileKind, mtime int64, model string) store.File {
	return store.File{
		ID: id, Path: path, Kind: kind, MTimeNS: mtime,
		Status: store.FileStatusIndexed, EmbedModelVersion: stringPointer(model),
	}
}

func withStatus(file store.File, status store.FileStatus) store.File {
	file.Status = status
	return file
}

func stringPointer(value string) *string { return &value }

func normalizedEmbedding(model string) QueryEmbedding {
	return QueryEmbedding{Values: []float32{1, 0}, ModelVersion: model}
}

func vectorHit(fileID int64, frameIndex int, timestamp *int64, score float32, model string) VectorHit {
	return VectorHit{
		Key: uint64(fileID)<<16 | uint64(frameIndex), FileID: fileID,
		FrameIndex: frameIndex, FrameTSMS: timestamp, Score: score, ModelVersion: model,
	}
}

func waitStarted(t *testing.T, started <-chan struct{}, route string) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s route did not start", route)
	}
}

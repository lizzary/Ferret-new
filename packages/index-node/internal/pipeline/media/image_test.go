package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"testing"
	"time"
)

func TestNewImageProcessorDefaultsAndValidation(t *testing.T) {
	processor, err := NewImageProcessor(ImageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if processor.config.Size != DefaultImageSize || processor.config.JPEGQuality != DefaultJPEGQuality ||
		processor.config.MaxPixels != DefaultImageMaxPixels || processor.config.BytesInflight != DefaultImageBytesInflight {
		t.Fatalf("defaults = %+v", processor.config)
	}

	tests := []ImageConfig{
		{Size: -1},
		{JPEGQuality: -1},
		{JPEGQuality: 101},
		{MaxPixels: -1},
		{BytesInflight: -1},
		{MaxPixels: 100, BytesInflight: 399},
	}
	for _, config := range tests {
		if _, err := NewImageProcessor(config); !errors.Is(err, ErrInvalidImageConfig) {
			t.Fatalf("NewImageProcessor(%+v) error = %v, want ErrInvalidImageConfig", config, err)
		}
	}
}

func TestImageProcessorMatchExtensionAndSignature(t *testing.T) {
	processor := mustImageProcessor(t, ImageConfig{})
	tests := []struct {
		name  string
		path  string
		sniff []byte
		want  bool
	}{
		{name: "JPEG extension", path: "photo.JPEG", want: true},
		{name: "PNG extension", path: "photo.png", want: true},
		{name: "GIF extension", path: "photo.gif", want: true},
		{name: "JPEG signature without extension", path: "photo", sniff: []byte{0xff, 0xd8, 0xff, 0xe0}, want: true},
		{name: "PNG signature without extension", path: "photo.bin", sniff: []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, want: true},
		{name: "GIF87a signature", path: "photo.bin", sniff: []byte("GIF87a"), want: true},
		{name: "GIF89a signature", path: "photo.bin", sniff: []byte("GIF89a"), want: true},
		{name: "unrelated", path: "notes.txt", sniff: []byte("plain text"), want: false},
		{name: "short PNG prefix", path: "photo.bin", sniff: []byte{0x89, 'P', 'N'}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := processor.Match(test.path, test.sniff); got != test.want {
				t.Fatalf("Match(%q, %x) = %v, want %v", test.path, test.sniff, got, test.want)
			}
		})
	}
}

func TestImageProcessorProcessSupportedFormats(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 6, 4))
	fillNRGBA(source, color.NRGBA{R: 30, G: 140, B: 220, A: 255})
	encoders := map[string]func(*testing.T, image.Image) []byte{
		"jpeg": encodeJPEG,
		"png":  encodePNG,
		"gif":  encodeGIF,
	}
	processor := mustImageProcessor(t, ImageConfig{Size: 8, JPEGQuality: 95})
	for name, encode := range encoders {
		t.Run(name, func(t *testing.T) {
			frame, err := processor.Process(context.Background(), bytes.NewReader(encode(t, source)))
			if err != nil {
				t.Fatal(err)
			}
			if frame.FrameIndex != 0 || frame.FrameTSMS != nil || len(frame.JPEG) == 0 {
				t.Fatalf("frame = %+v", frame)
			}
			decodedConfig, format, err := image.DecodeConfig(bytes.NewReader(frame.JPEG))
			if err != nil {
				t.Fatal(err)
			}
			if format != jpegFormat || decodedConfig.Width != 8 || decodedConfig.Height != 8 {
				t.Fatalf("normalized image = format %q, size %dx%d", format, decodedConfig.Width, decodedConfig.Height)
			}
		})
	}
}

func TestImageProcessorCenterCropsBeforeResize(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 7, 3))
	for y := 0; y < 3; y++ {
		for x := 0; x < 7; x++ {
			value := color.NRGBA{R: 240, A: 255}
			if x >= 2 && x <= 4 {
				value = color.NRGBA{G: 230, A: 255}
			} else if x > 4 {
				value = color.NRGBA{B: 240, A: 255}
			}
			source.SetNRGBA(x, y, value)
		}
	}
	processor := mustImageProcessor(t, ImageConfig{Size: 3, JPEGQuality: 100})
	frame, err := processor.Process(context.Background(), bytes.NewReader(encodePNG(t, source)))
	if err != nil {
		t.Fatal(err)
	}
	normalized := decodeJPEG(t, frame.JPEG)
	for y := 0; y < 3; y++ {
		for x := 0; x < 3; x++ {
			red, green, blue, _ := rgba8(normalized.At(x, y))
			if green < 190 || green <= red+80 || green <= blue+80 {
				t.Fatalf("pixel (%d,%d) = rgb(%d,%d,%d), center crop leaked an outer band", x, y, red, green, blue)
			}
		}
	}
}

func TestImageProcessorCompositesAlphaOverWhite(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 4, 4))
	fillNRGBA(source, color.NRGBA{R: 255, A: 128})
	processor := mustImageProcessor(t, ImageConfig{Size: 4, JPEGQuality: 100})
	frame, err := processor.Process(context.Background(), bytes.NewReader(encodePNG(t, source)))
	if err != nil {
		t.Fatal(err)
	}
	red, green, blue, alpha := rgba8(decodeJPEG(t, frame.JPEG).At(2, 2))
	if red < 240 || green < 105 || green > 150 || blue < 105 || blue > 150 || alpha != 255 {
		t.Fatalf("composited pixel = rgba(%d,%d,%d,%d), want opaque pink over white", red, green, blue, alpha)
	}
}

func TestImageProcessorGIFUsesFirstFrame(t *testing.T) {
	palette := color.Palette{color.RGBA{R: 240, A: 255}, color.RGBA{B: 240, A: 255}}
	first := image.NewPaletted(image.Rect(0, 0, 4, 4), palette)
	second := image.NewPaletted(image.Rect(0, 0, 4, 4), palette)
	for index := range second.Pix {
		second.Pix[index] = 1
	}
	var encoded bytes.Buffer
	if err := gif.EncodeAll(&encoded, &gif.GIF{Image: []*image.Paletted{first, second}, Delay: []int{1, 1}}); err != nil {
		t.Fatal(err)
	}
	processor := mustImageProcessor(t, ImageConfig{Size: 4, JPEGQuality: 100})
	frame, err := processor.Process(context.Background(), bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	red, _, blue, _ := rgba8(decodeJPEG(t, frame.JPEG).At(2, 2))
	if red < 190 || red <= blue+100 {
		t.Fatalf("GIF output = red %d blue %d, want first red frame", red, blue)
	}
}

func TestImageProcessorAppliesJPEGEXIFOrientationsBeforeCrop(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 80, 60))
	fillNRGBA(source, color.NRGBA{A: 255})
	quadrants := map[string]color.NRGBA{
		"red":    {R: 240, A: 255},
		"green":  {G: 230, A: 255},
		"blue":   {B: 240, A: 255},
		"yellow": {R: 230, G: 230, A: 255},
	}
	for y := 0; y < 60; y++ {
		for x := 10; x < 70; x++ {
			name := "red"
			switch {
			case x >= 40 && y < 30:
				name = "green"
			case x < 40 && y >= 30:
				name = "blue"
			case x >= 40 && y >= 30:
				name = "yellow"
			}
			source.SetNRGBA(x, y, quadrants[name])
		}
	}
	original := encodeJPEG(t, source)
	expected := map[int][4]string{
		1: {"red", "green", "blue", "yellow"},
		2: {"green", "red", "yellow", "blue"},
		3: {"yellow", "blue", "green", "red"},
		4: {"blue", "yellow", "red", "green"},
		5: {"red", "blue", "green", "yellow"},
		6: {"blue", "red", "yellow", "green"},
		7: {"yellow", "green", "blue", "red"},
		8: {"green", "yellow", "red", "blue"},
	}
	processor := mustImageProcessor(t, ImageConfig{Size: 60, JPEGQuality: 100})
	points := []image.Point{{15, 15}, {45, 15}, {15, 45}, {45, 45}}
	for orientation := 1; orientation <= 8; orientation++ {
		t.Run(string(rune('0'+orientation)), func(t *testing.T) {
			encoded := addEXIFOrientation(t, original, uint16(orientation), binary.LittleEndian)
			if got := jpegEXIFOrientation(encoded); got != orientation {
				t.Fatalf("jpegEXIFOrientation() = %d, want %d", got, orientation)
			}
			frame, err := processor.Process(context.Background(), bytes.NewReader(encoded))
			if err != nil {
				t.Fatal(err)
			}
			output := decodeJPEG(t, frame.JPEG)
			for index, point := range points {
				if got := dominantColor(output.At(point.X, point.Y)); got != expected[orientation][index] {
					t.Fatalf("orientation %d quadrant %d = %s, want %s", orientation, index, got, expected[orientation][index])
				}
			}
		})
	}
}

func TestJPEGEXIFOrientationSupportsBigEndianTIFF(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 2, 3))
	encoded := addEXIFOrientation(t, encodeJPEG(t, source), 8, binary.BigEndian)
	if got := jpegEXIFOrientation(encoded); got != 8 {
		t.Fatalf("jpegEXIFOrientation(big endian) = %d, want 8", got)
	}
}

func TestImageProcessorRejectsPixelLimitAndBadImages(t *testing.T) {
	processor := mustImageProcessor(t, ImageConfig{Size: 4, MaxPixels: 100})
	tooLarge := image.NewNRGBA(image.Rect(0, 0, 11, 10))
	if _, err := processor.Process(context.Background(), bytes.NewReader(encodePNG(t, tooLarge))); !errors.Is(err, ErrImageTooLarge) {
		t.Fatalf("large image error = %v, want ErrImageTooLarge", err)
	}

	badImages := [][]byte{
		[]byte("not an image"),
		{0xff, 0xd8, 0xff, 0xe0, 0x00},
		{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'},
	}
	for _, encoded := range badImages {
		if _, err := processor.Process(context.Background(), bytes.NewReader(encoded)); !errors.Is(err, ErrUnsupportedImage) {
			t.Fatalf("bad image %x error = %v, want ErrUnsupportedImage", encoded, err)
		}
	}
}

func TestImageProcessorHonorsCancellation(t *testing.T) {
	processor := mustImageProcessor(t, ImageConfig{Size: 8})
	source := image.NewNRGBA(image.Rect(0, 0, 16, 16))
	encoded := encodePNG(t, source)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := processor.Process(canceled, bytes.NewReader(encoded)); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Process error = %v, want context.Canceled", err)
	}

	duringRead, cancelDuringRead := context.WithCancel(context.Background())
	reader := &cancelAfterReadReader{reader: bytes.NewReader(encoded), cancel: cancelDuringRead}
	if _, err := processor.Process(duringRead, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("read-canceled Process error = %v, want context.Canceled", err)
	}
}

func TestImageProcessorSharesAndReleasesDecodedByteBudget(t *testing.T) {
	const decodedBytes = int64(4 * 4 * 4)
	processor := mustImageProcessor(t, ImageConfig{
		Size: 4, MaxPixels: 16, BytesInflight: decodedBytes,
	})
	encoded := encodePNG(t, image.NewNRGBA(image.Rect(0, 0, 4, 4)))
	if !processor.decodedBytes.TryAcquire(decodedBytes) {
		t.Fatal("failed to reserve the complete decoded-byte budget")
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := processor.Process(waitCtx, bytes.NewReader(encoded)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("budget-blocked Process error = %v, want context deadline", err)
	}
	processor.decodedBytes.Release(decodedBytes)

	if _, err := processor.Process(context.Background(), bytes.NewReader(encoded)); err != nil {
		t.Fatalf("Process after budget release: %v", err)
	}
	assertCompleteBudgetAvailable(t, processor, decodedBytes)

	truncated := encoded[:min(40, len(encoded))]
	if _, _, err := image.DecodeConfig(bytes.NewReader(truncated)); err != nil {
		t.Fatalf("truncated fixture must retain a decodable header: %v", err)
	}
	if _, err := processor.Process(context.Background(), bytes.NewReader(truncated)); !errors.Is(err, ErrUnsupportedImage) {
		t.Fatalf("truncated Process error = %v, want ErrUnsupportedImage", err)
	}
	assertCompleteBudgetAvailable(t, processor, decodedBytes)
}

func TestImageProcessorRecoversPanicsAndReleasesBudget(t *testing.T) {
	const decodedBytes = int64(4 * 4 * 4)
	processor := mustImageProcessor(t, ImageConfig{
		Size: 4, MaxPixels: 16, BytesInflight: decodedBytes,
	})
	encoded := encodePNG(t, image.NewNRGBA(image.Rect(0, 0, 4, 4)))
	reader := &panicAfterNReader{encoded: encoded, panicAfter: 40}
	if _, err := processor.Process(context.Background(), reader); !errors.Is(err, ErrImagePanic) {
		t.Fatalf("panic Process error = %v, want ErrImagePanic", err)
	}
	assertCompleteBudgetAvailable(t, processor, decodedBytes)
}

func TestImageProcessorRejectsNilInputs(t *testing.T) {
	processor := mustImageProcessor(t, ImageConfig{})
	if _, err := processor.Process(nil, bytes.NewReader(nil)); err == nil {
		t.Fatal("Process accepted a nil context")
	}
	if _, err := processor.Process(context.Background(), nil); err == nil {
		t.Fatal("Process accepted a nil reader")
	}
	var nilProcessor *ImageProcessor
	if _, err := nilProcessor.Process(context.Background(), bytes.NewReader(nil)); err == nil {
		t.Fatal("Process accepted a nil processor")
	}
}

func mustImageProcessor(t *testing.T, config ImageConfig) *ImageProcessor {
	t.Helper()
	processor, err := NewImageProcessor(config)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func fillNRGBA(target *image.NRGBA, value color.NRGBA) {
	for y := target.Bounds().Min.Y; y < target.Bounds().Max.Y; y++ {
		for x := target.Bounds().Min.X; x < target.Bounds().Max.X; x++ {
			target.SetNRGBA(x, y, value)
		}
	}
}

func encodePNG(t *testing.T, source image.Image) []byte {
	t.Helper()
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, source); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func encodeJPEG(t *testing.T, source image.Image) []byte {
	t.Helper()
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, source, &jpeg.Options{Quality: 100}); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func encodeGIF(t *testing.T, source image.Image) []byte {
	t.Helper()
	var encoded bytes.Buffer
	if err := gif.Encode(&encoded, source, nil); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func decodeJPEG(t *testing.T, encoded []byte) image.Image {
	t.Helper()
	decoded, err := jpeg.Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func rgba8(value color.Color) (uint8, uint8, uint8, uint8) {
	red, green, blue, alpha := value.RGBA()
	return uint8(red >> 8), uint8(green >> 8), uint8(blue >> 8), uint8(alpha >> 8)
}

func dominantColor(value color.Color) string {
	red, green, blue, _ := rgba8(value)
	switch {
	case red > 170 && green > 170 && blue < 100:
		return "yellow"
	case red > 170 && green < 120 && blue < 120:
		return "red"
	case green > 160 && red < 120 && blue < 120:
		return "green"
	case blue > 170 && red < 120 && green < 120:
		return "blue"
	default:
		return "unknown"
	}
}

func addEXIFOrientation(t *testing.T, encoded []byte, orientation uint16, order binary.ByteOrder) []byte {
	t.Helper()
	if len(encoded) < 2 || encoded[0] != 0xff || encoded[1] != 0xd8 {
		t.Fatal("EXIF fixture is not JPEG")
	}
	tiff := make([]byte, 26)
	if order == binary.LittleEndian {
		copy(tiff[:2], "II")
	} else {
		copy(tiff[:2], "MM")
	}
	order.PutUint16(tiff[2:4], 42)
	order.PutUint32(tiff[4:8], 8)
	order.PutUint16(tiff[8:10], 1)
	entry := tiff[10:22]
	order.PutUint16(entry[:2], 0x0112)
	order.PutUint16(entry[2:4], 3)
	order.PutUint32(entry[4:8], 1)
	order.PutUint16(entry[8:10], orientation)

	payload := append([]byte{'E', 'x', 'i', 'f', 0, 0}, tiff...)
	segment := make([]byte, 4+len(payload))
	segment[0], segment[1] = 0xff, 0xe1
	binary.BigEndian.PutUint16(segment[2:4], uint16(len(payload)+2))
	copy(segment[4:], payload)
	result := make([]byte, 0, len(encoded)+len(segment))
	result = append(result, encoded[:2]...)
	result = append(result, segment...)
	result = append(result, encoded[2:]...)
	return result
}

func assertCompleteBudgetAvailable(t *testing.T, processor *ImageProcessor, bytes int64) {
	t.Helper()
	if !processor.decodedBytes.TryAcquire(bytes) {
		t.Fatal("decoded-byte budget was not fully released")
	}
	processor.decodedBytes.Release(bytes)
}

type cancelAfterReadReader struct {
	reader io.Reader
	cancel context.CancelFunc
	done   bool
}

type panicAfterNReader struct {
	encoded    []byte
	offset     int
	panicAfter int
}

func (reader *panicAfterNReader) Read(buffer []byte) (int, error) {
	if reader.offset >= reader.panicAfter {
		panic("fixture reader panic")
	}
	if reader.offset >= len(reader.encoded) {
		return 0, io.EOF
	}
	limit := min(len(buffer), reader.panicAfter-reader.offset, len(reader.encoded)-reader.offset)
	count := copy(buffer[:limit], reader.encoded[reader.offset:reader.offset+limit])
	reader.offset += count
	return count, nil
}

func (reader *cancelAfterReadReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if !reader.done {
		reader.done = true
		reader.cancel()
	}
	return count, err
}

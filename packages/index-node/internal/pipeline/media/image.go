// Package media prepares images for the embedding pipeline.
package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"math"
	"path/filepath"
	"strings"

	"github.com/lizzary/index-node/internal/pipeline"
	"golang.org/x/sync/semaphore"
)

const (
	DefaultImageSize          = 384
	DefaultJPEGQuality        = 90
	DefaultImageMaxPixels     = int64(25_000_000)
	DefaultImageBytesInflight = int64(256 << 20)
	decodedBytesPerPixel      = int64(4)
	jpegFormat                = "jpeg"
	pngFormat                 = "png"
	gifFormat                 = "gif"
)

var (
	ErrInvalidImageConfig = errors.New("media image: invalid configuration")
	ErrUnsupportedImage   = errors.New("media image: unsupported or corrupt image")
	ErrImageTooLarge      = errors.New("media image: decoded dimensions exceed pixel limit")
	ErrImagePanic         = errors.New("media image: recovered panic")
)

// ImageConfig controls the normalized JPEG sent to the embedding service.
// Zero values select the production defaults.
type ImageConfig struct {
	Size          int
	JPEGQuality   int
	MaxPixels     int64
	BytesInflight int64
}

// ImageProcessor validates, decodes, center-crops, composites and resizes
// still images. It is immutable and safe for concurrent use.
type ImageProcessor struct {
	config       ImageConfig
	decodedBytes *semaphore.Weighted
}

// NewImageProcessor constructs an image processor. Negative values are not
// treated as defaults so malformed resolved configuration cannot be hidden.
func NewImageProcessor(config ImageConfig) (*ImageProcessor, error) {
	if config.Size == 0 {
		config.Size = DefaultImageSize
	}
	if config.JPEGQuality == 0 {
		config.JPEGQuality = DefaultJPEGQuality
	}
	if config.MaxPixels == 0 {
		config.MaxPixels = DefaultImageMaxPixels
	}
	if config.BytesInflight == 0 {
		config.BytesInflight = DefaultImageBytesInflight
	}
	if config.Size < 1 {
		return nil, fmt.Errorf("%w: size must be positive", ErrInvalidImageConfig)
	}
	if config.JPEGQuality < 1 || config.JPEGQuality > 100 {
		return nil, fmt.Errorf("%w: JPEG quality must be between 1 and 100", ErrInvalidImageConfig)
	}
	if config.MaxPixels < 1 {
		return nil, fmt.Errorf("%w: max pixels must be positive", ErrInvalidImageConfig)
	}
	if config.MaxPixels > math.MaxInt64/decodedBytesPerPixel {
		return nil, fmt.Errorf("%w: max pixels is too large", ErrInvalidImageConfig)
	}
	if config.BytesInflight < 1 {
		return nil, fmt.Errorf("%w: bytes in flight must be positive", ErrInvalidImageConfig)
	}
	if config.BytesInflight < config.MaxPixels*decodedBytesPerPixel {
		return nil, fmt.Errorf("%w: bytes in flight must hold one maximum-size decoded image", ErrInvalidImageConfig)
	}
	return &ImageProcessor{config: config, decodedBytes: semaphore.NewWeighted(config.BytesInflight)}, nil
}

// Match recognizes the supported extensions as well as JPEG, PNG and GIF
// signatures. Signature matching permits correctly encoded images whose name
// has no useful extension; extension matching lets Process return a precise
// decode error for a damaged image instead of silently routing it as text.
func (processor *ImageProcessor) Match(path string, sniff []byte) bool {
	if supportedImageFormat(sniff) != "" {
		return true
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".jpg", ".jpeg", ".jpe", ".jfif", ".png", ".gif":
		return true
	default:
		return false
	}
}

// Process converts one supported still image into a square JPEG frame. The
// standard GIF decoder intentionally returns only the first animation frame.
func (processor *ImageProcessor) Process(ctx context.Context, reader io.Reader) (frame pipeline.Frame, returnErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			frame = pipeline.Frame{}
			returnErr = fmt.Errorf("%w: %v", ErrImagePanic, recovered)
		}
	}()
	if processor == nil {
		return pipeline.Frame{}, errors.New("media image: nil processor")
	}
	if ctx == nil {
		return pipeline.Frame{}, errors.New("media image: context is required")
	}
	if reader == nil {
		return pipeline.Frame{}, errors.New("media image: reader is required")
	}
	if err := ctx.Err(); err != nil {
		return pipeline.Frame{}, err
	}

	contextual := &contextReader{ctx: ctx, reader: reader}
	var consumed bytes.Buffer
	decodedConfig, format, err := image.DecodeConfig(io.TeeReader(contextual, &consumed))
	if err != nil {
		return pipeline.Frame{}, decodeError(err)
	}
	if !isSupportedFormat(format) {
		return pipeline.Frame{}, fmt.Errorf("%w: format %q", ErrUnsupportedImage, format)
	}
	if decodedConfig.Width <= 0 || decodedConfig.Height <= 0 {
		return pipeline.Frame{}, fmt.Errorf("%w: invalid dimensions %dx%d", ErrUnsupportedImage, decodedConfig.Width, decodedConfig.Height)
	}
	if exceedsPixelLimit(decodedConfig.Width, decodedConfig.Height, processor.config.MaxPixels) {
		return pipeline.Frame{}, fmt.Errorf("%w: %dx%d exceeds %d pixels",
			ErrImageTooLarge, decodedConfig.Width, decodedConfig.Height, processor.config.MaxPixels)
	}
	if err := ctx.Err(); err != nil {
		return pipeline.Frame{}, err
	}
	decodedBytes := int64(decodedConfig.Width) * int64(decodedConfig.Height) * decodedBytesPerPixel
	if err := processor.decodedBytes.Acquire(ctx, decodedBytes); err != nil {
		return pipeline.Frame{}, err
	}
	defer processor.decodedBytes.Release(decodedBytes)

	decoded, decodedFormat, err := image.Decode(io.MultiReader(bytes.NewReader(consumed.Bytes()), contextual))
	if err != nil {
		return pipeline.Frame{}, decodeError(err)
	}
	if decodedFormat != format || !isSupportedFormat(decodedFormat) {
		return pipeline.Frame{}, fmt.Errorf("%w: inconsistent decoded format %q", ErrUnsupportedImage, decodedFormat)
	}
	if err := ctx.Err(); err != nil {
		return pipeline.Frame{}, err
	}
	if format == jpegFormat {
		decoded = applyEXIFOrientation(decoded, jpegEXIFOrientation(consumed.Bytes()))
	}

	normalized, err := resizeCenteredSquare(ctx, decoded, processor.config.Size)
	if err != nil {
		return pipeline.Frame{}, err
	}
	var output bytes.Buffer
	if err := ctx.Err(); err != nil {
		return pipeline.Frame{}, err
	}
	if err := jpeg.Encode(&output, normalized, &jpeg.Options{Quality: processor.config.JPEGQuality}); err != nil {
		return pipeline.Frame{}, fmt.Errorf("media image: encode normalized JPEG: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return pipeline.Frame{}, err
	}
	return pipeline.Frame{FrameIndex: 0, JPEG: output.Bytes()}, nil
}

// jpegEXIFOrientation reads the standard APP1 Exif/TIFF orientation tag. Exif
// metadata is optional and non-authoritative: malformed or unsupported APP1
// data leaves pixels in their encoded orientation instead of rejecting an
// otherwise decodable image.
func jpegEXIFOrientation(encodedPrefix []byte) int {
	if len(encodedPrefix) < 4 || encodedPrefix[0] != 0xff || encodedPrefix[1] != 0xd8 {
		return 1
	}
	for offset := 2; offset < len(encodedPrefix); {
		if encodedPrefix[offset] != 0xff {
			offset++
			continue
		}
		for offset < len(encodedPrefix) && encodedPrefix[offset] == 0xff {
			offset++
		}
		if offset >= len(encodedPrefix) {
			return 1
		}
		marker := encodedPrefix[offset]
		offset++
		switch {
		case marker == 0x00:
			continue
		case marker == 0xd8 || marker == 0xd9 || marker == 0x01 || marker >= 0xd0 && marker <= 0xd7:
			continue
		case marker == 0xda:
			return 1
		}
		if offset+2 > len(encodedPrefix) {
			return 1
		}
		length := int(binary.BigEndian.Uint16(encodedPrefix[offset : offset+2]))
		if length < 2 || offset+length > len(encodedPrefix) {
			return 1
		}
		if marker == 0xe1 {
			if orientation := tiffOrientation(encodedPrefix[offset+2 : offset+length]); orientation != 1 {
				return orientation
			}
		}
		offset += length
	}
	return 1
}

func tiffOrientation(app1 []byte) int {
	if len(app1) < 14 || !bytes.Equal(app1[:6], []byte{'E', 'x', 'i', 'f', 0, 0}) {
		return 1
	}
	tiff := app1[6:]
	var order binary.ByteOrder
	switch string(tiff[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 1
	}
	if order.Uint16(tiff[2:4]) != 42 {
		return 1
	}
	ifdOffset := uint64(order.Uint32(tiff[4:8]))
	if ifdOffset > uint64(len(tiff)-2) {
		return 1
	}
	entryCount := uint64(order.Uint16(tiff[ifdOffset : ifdOffset+2]))
	entriesOffset := ifdOffset + 2
	for index := uint64(0); index < entryCount; index++ {
		entryOffset := entriesOffset + index*12
		if entryOffset > uint64(len(tiff)) || uint64(len(tiff))-entryOffset < 12 {
			return 1
		}
		entry := tiff[entryOffset : entryOffset+12]
		if order.Uint16(entry[:2]) != 0x0112 || order.Uint16(entry[2:4]) != 3 || order.Uint32(entry[4:8]) != 1 {
			continue
		}
		orientation := int(order.Uint16(entry[8:10]))
		if orientation >= 1 && orientation <= 8 {
			return orientation
		}
		return 1
	}
	return 1
}

func applyEXIFOrientation(source image.Image, orientation int) image.Image {
	if orientation <= 1 || orientation > 8 {
		return source
	}
	return exifOrientedImage{source: source, orientation: orientation}
}

type exifOrientedImage struct {
	source      image.Image
	orientation int
}

func (oriented exifOrientedImage) ColorModel() color.Model { return oriented.source.ColorModel() }

func (oriented exifOrientedImage) Bounds() image.Rectangle {
	bounds := oriented.source.Bounds()
	if oriented.orientation >= 5 {
		return image.Rect(0, 0, bounds.Dy(), bounds.Dx())
	}
	return image.Rect(0, 0, bounds.Dx(), bounds.Dy())
}

func (oriented exifOrientedImage) At(x, y int) color.Color {
	if !image.Pt(x, y).In(oriented.Bounds()) {
		return color.RGBA{}
	}
	bounds := oriented.source.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	var sourceX, sourceY int
	switch oriented.orientation {
	case 2:
		sourceX, sourceY = width-1-x, y
	case 3:
		sourceX, sourceY = width-1-x, height-1-y
	case 4:
		sourceX, sourceY = x, height-1-y
	case 5:
		sourceX, sourceY = y, x
	case 6:
		sourceX, sourceY = y, height-1-x
	case 7:
		sourceX, sourceY = width-1-y, height-1-x
	case 8:
		sourceX, sourceY = width-1-y, x
	default:
		sourceX, sourceY = x, y
	}
	return oriented.source.At(bounds.Min.X+sourceX, bounds.Min.Y+sourceY)
}

func supportedImageFormat(sniff []byte) string {
	switch {
	case len(sniff) >= 3 && sniff[0] == 0xff && sniff[1] == 0xd8 && sniff[2] == 0xff:
		return jpegFormat
	case len(sniff) >= 8 && bytes.Equal(sniff[:8], []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}):
		return pngFormat
	case len(sniff) >= 6 && (bytes.Equal(sniff[:6], []byte("GIF87a")) || bytes.Equal(sniff[:6], []byte("GIF89a"))):
		return gifFormat
	default:
		return ""
	}
}

func isSupportedFormat(format string) bool {
	return format == jpegFormat || format == pngFormat || format == gifFormat
}

func decodeError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrUnsupportedImage, err)
}

func exceedsPixelLimit(width, height int, maxPixels int64) bool {
	return int64(width) > maxPixels/int64(height)
}

// resizeCenteredSquare combines crop, alpha compositing and bilinear scaling
// in one pass. Avoiding a full-size crop buffer keeps memory bounded by the
// decoder image plus the fixed embedding input image.
func resizeCenteredSquare(ctx context.Context, source image.Image, size int) (*image.RGBA, error) {
	bounds := source.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("%w: decoded image has empty bounds", ErrUnsupportedImage)
	}
	side := min(width, height)
	cropMinX := bounds.Min.X + (width-side)/2
	cropMinY := bounds.Min.Y + (height-side)/2
	cropMaxX := cropMinX + side - 1
	cropMaxY := cropMinY + side - 1

	target := image.NewRGBA(image.Rect(0, 0, size, size))
	scale := float64(side) / float64(size)
	for targetY := 0; targetY < size; targetY++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sourceY := float64(cropMinY) + (float64(targetY)+0.5)*scale - 0.5
		y0, y1, yFraction := interpolationCoordinates(sourceY, cropMinY, cropMaxY)
		for targetX := 0; targetX < size; targetX++ {
			sourceX := float64(cropMinX) + (float64(targetX)+0.5)*scale - 0.5
			x0, x1, xFraction := interpolationCoordinates(sourceX, cropMinX, cropMaxX)

			red00, green00, blue00 := opaqueRGB(source.At(x0, y0))
			red10, green10, blue10 := opaqueRGB(source.At(x1, y0))
			red01, green01, blue01 := opaqueRGB(source.At(x0, y1))
			red11, green11, blue11 := opaqueRGB(source.At(x1, y1))
			target.SetRGBA(targetX, targetY, color.RGBA{
				R: interpolateChannel(red00, red10, red01, red11, xFraction, yFraction),
				G: interpolateChannel(green00, green10, green01, green11, xFraction, yFraction),
				B: interpolateChannel(blue00, blue10, blue01, blue11, xFraction, yFraction),
				A: 0xff,
			})
		}
	}
	return target, nil
}

func interpolationCoordinates(value float64, lower, upper int) (int, int, float64) {
	if value <= float64(lower) {
		return lower, lower, 0
	}
	if value >= float64(upper) {
		return upper, upper, 0
	}
	first := int(math.Floor(value))
	return first, first + 1, value - float64(first)
}

// color.Color.RGBA returns alpha-premultiplied components. Adding the white
// background contribution produces the exact source-over-white result while
// keeping interpolation in the opaque color space.
func opaqueRGB(value color.Color) (float64, float64, float64) {
	red, green, blue, alpha := value.RGBA()
	white := uint32(0xffff) - alpha
	return float64(red + white), float64(green + white), float64(blue + white)
}

func interpolateChannel(c00, c10, c01, c11, xFraction, yFraction float64) uint8 {
	top := c00 + (c10-c00)*xFraction
	bottom := c01 + (c11-c01)*xFraction
	value := top + (bottom-top)*yFraction
	value = math.Round(value / 257)
	if value < 0 {
		return 0
	}
	if value > 255 {
		return 255
	}
	return uint8(value)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := reader.reader.Read(buffer)
	if count == 0 {
		if contextErr := reader.ctx.Err(); contextErr != nil {
			return 0, contextErr
		}
	}
	return count, err
}

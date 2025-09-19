package main

/*
import (
	"database/sql"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/corona10/goimagehash"
	pigo "github.com/esimov/pigo/core"
	_ "github.com/mattn/go-sqlite3"
)

// FaceDetector wraps the Pigo classifier
type FaceDetector struct {
	classifier *pigo.Pigo
}

// NewFaceDetector creates a new face detector instance
func NewFaceDetector(cascadeFile string) (*FaceDetector, error) {
	cascadeData, err := ioutil.ReadFile(cascadeFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read cascade file: %v", err)
	}

	p := pigo.NewPigo()
	classifier, err := p.Unpack(cascadeData)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack cascade: %v", err)
	}

	return &FaceDetector{
		classifier: classifier,
	}, nil
}

// DetectFaces detects faces in an image and returns detection results
func (fd *FaceDetector) DetectFaces(img image.Image, minSize, maxSize int, shiftFactor, scaleFactor float64, iouThreshold float64) []pigo.Detection {
	// Convert image to grayscale
	src := pigo.ImgToNRGBA(img)
	pixels := pigo.RgbToGrayscale(src)
	cols, rows := src.Bounds().Max.X, src.Bounds().Max.Y

	// Canny edge detection parameters
	cParams := pigo.CascadeParams{
		MinSize:     minSize,
		MaxSize:     maxSize,
		ShiftFactor: shiftFactor,
		ScaleFactor: scaleFactor,
		ImageParams: pigo.ImageParams{
			Pixels: pixels,
			Rows:   rows,
			Cols:   cols,
			Dim:    cols,
		},
	}

	// Run the classifier over the obtained leaf nodes and return the detection results
	dets := fd.classifier.RunCascade(cParams, 0.0)

	// Calculate the intersection over union (IoU) of two clusters
	dets = fd.classifier.ClusterDetections(dets, iouThreshold)

	return dets
}

// DrawDetections draws bounding boxes around detected faces
func DrawDetections(img image.Image, detections []pigo.Detection) image.Image {
	// Create a new RGBA image for drawing
	bounds := img.Bounds()
	dst := image.NewRGBA(bounds)
	draw.Draw(dst, bounds, img, bounds.Min, draw.Src)

	// Draw rectangles around detected faces
	for _, det := range detections {
		if det.Q > 150.0 { // Quality threshold (very high for best detection)
			// Calculate rectangle coordinates
			x1 := det.Col - det.Scale/2
			y1 := det.Row - det.Scale/2
			x2 := det.Col + det.Scale/2
			y2 := det.Row + det.Scale/2

			// Draw rectangle
			drawRect(dst, x1, y1, x2, y2, color.RGBA{255, 0, 0, 255})
		}
	}

	return dst
}

// drawRect draws a rectangle on the image
func drawRect(img *image.RGBA, x1, y1, x2, y2 int, col color.RGBA) {
	// Draw horizontal lines
	for x := x1; x <= x2; x++ {
		if x >= 0 && x < img.Bounds().Max.X {
			if y1 >= 0 && y1 < img.Bounds().Max.Y {
				img.Set(x, y1, col)
			}
			if y2 >= 0 && y2 < img.Bounds().Max.Y {
				img.Set(x, y2, col)
			}
		}
	}

	// Draw vertical lines
	for y := y1; y <= y2; y++ {
		if y >= 0 && y < img.Bounds().Max.Y {
			if x1 >= 0 && x1 < img.Bounds().Max.X {
				img.Set(x1, y, col)
			}
			if x2 >= 0 && x2 < img.Bounds().Max.X {
				img.Set(x2, y, col)
			}
		}
	}
}

// loadImage loads an image from file
func loadImage(filename string) (image.Image, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(filename))
	var img image.Image

	switch ext {
	case ".jpg", ".jpeg":
		img, err = jpeg.Decode(file)
	case ".png":
		img, err = png.Decode(file)
	default:
		return nil, fmt.Errorf("unsupported image format: %s", ext)
	}

	return img, err
}

// saveImage saves an image to file
func saveImage(img image.Image, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg":
		return jpeg.Encode(file, img, &jpeg.Options{Quality: 95})
	case ".png":
		return png.Encode(file, img)
	default:
		return fmt.Errorf("unsupported output format: %s", ext)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  go run . <image_file> [output_file]  (detect faces)")
		fmt.Println("  go run . <image1> <image2>  (compare faces)")
		fmt.Println("  go run . add <name> <image_file>  (add face to database)")
		fmt.Println("  go run . recognize <image_file>  (recognize face from database)")
		os.Exit(1)
	}

	// Open database
	db, err := sql.Open("sqlite3", "./faces.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS faces (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		hash TEXT NOT NULL,
		image_path TEXT
	)`)
	if err != nil {
		log.Fatal(err)
	}

	// Create face detector
	detector, err := NewFaceDetector("./cascade/facefinder")
	if err != nil {
		log.Fatalf("Failed to create face detector: %v", err)
	}

	if len(os.Args) > 2 && os.Args[1] == "add" {
		if len(os.Args) < 4 {
			fmt.Println("Usage: go run . add <name> <image_file>")
			os.Exit(1)
		}
		name := os.Args[2]
		imageFile := os.Args[3]
		addFace(db, detector, name, imageFile)
	} else if len(os.Args) > 2 && os.Args[1] == "recognize" {
		if len(os.Args) < 3 {
			fmt.Println("Usage: go run . recognize <image_file>")
			os.Exit(1)
		}
		imageFile := os.Args[2]
		recognizeFace(db, detector, imageFile)
	} else {
		// Original logic for detection/comparison
		imageFile1 := os.Args[1]
		var imageFile2 string
		var outputFile string

		if len(os.Args) > 2 {
			// Check if second arg is an image file
			if strings.HasSuffix(os.Args[2], ".jpg") || strings.HasSuffix(os.Args[2], ".jpeg") || strings.HasSuffix(os.Args[2], ".png") {
				imageFile2 = os.Args[2]
				if len(os.Args) > 3 {
					outputFile = os.Args[3]
				}
			} else {
				outputFile = os.Args[2]
			}
		}

		if outputFile == "" {
			ext := filepath.Ext(imageFile1)
			outputFile = strings.TrimSuffix(imageFile1, ext) + "_detected" + ext
		}

		// Process first image
		hashes1 := processImage(detector, imageFile1, outputFile)

		if imageFile2 != "" {
			// Process second image for comparison
			outputFile2 := strings.TrimSuffix(imageFile2, filepath.Ext(imageFile2)) + "_detected" + filepath.Ext(imageFile2)
			hashes2 := processImage(detector, imageFile2, outputFile2)

			// Compare faces
			compareFaces(hashes1, hashes2)
		}
	}
}

func processImage(detector *FaceDetector, imageFile, outputFile string) []*goimagehash.ImageHash {
	// Load image
	img, err := loadImage(imageFile)
	if err != nil {
		log.Fatalf("Failed to load image %s: %v", imageFile, err)
	}

	fmt.Printf("Image %s loaded: %dx%d\n", imageFile, img.Bounds().Dx(), img.Bounds().Dy())

	// Detection parameters
	minSize := 10       // Minimum face size
	maxSize := 2000     // Maximum face size
	shiftFactor := 0.05 // How much to shift the detection window (0.05 = 5%)
	scaleFactor := 1.05 // How much to scale between detection sizes
	iouThreshold := 0.4 // Intersection over Union threshold for clustering

	// Detect faces
	fmt.Printf("Detecting faces in %s...\n", imageFile)
	detections := detector.DetectFaces(img, minSize, maxSize, shiftFactor, scaleFactor, iouThreshold)

	// Filter detections by quality - very restrictive threshold
	var validDetections []pigo.Detection
	for _, det := range detections {
		if det.Q > 150.0 { // Very high quality threshold to get only the best detection
			validDetections = append(validDetections, det)
		}
	}

	fmt.Printf("Found %d face(s) in %s\n", len(validDetections), imageFile)

	// Print detection details
	for i, det := range validDetections {
		fmt.Printf("Face %d: Position(x=%d, y=%d), Size=%d, Quality=%.2f\n",
			i+1, det.Col, det.Row, det.Scale, det.Q)
	}

	var faceHashes []*goimagehash.ImageHash

	// Crop and save individual faces
	for i, det := range validDetections {
		// Calculate crop coordinates
		x1 := det.Col - det.Scale/2
		y1 := det.Row - det.Scale/2
		x2 := det.Col + det.Scale/2
		y2 := det.Row + det.Scale/2

		// Ensure coordinates are within bounds
		if x1 < 0 {
			x1 = 0
		}
		if y1 < 0 {
			y1 = 0
		}
		if x2 > img.Bounds().Max.X {
			x2 = img.Bounds().Max.X
		}
		if y2 > img.Bounds().Max.Y {
			y2 = img.Bounds().Max.Y
		}

		// Crop the face
		faceRect := image.Rect(x1, y1, x2, y2)
		faceImg := img.(interface {
			SubImage(image.Rectangle) image.Image
		}).SubImage(faceRect)

		// Save the face
		faceFilename := fmt.Sprintf("face_%s_%d.jpg", strings.TrimSuffix(filepath.Base(imageFile), filepath.Ext(imageFile)), i+1)
		if err := saveImage(faceImg, faceFilename); err != nil {
			log.Printf("Failed to save face %d: %v", i+1, err)
		} else {
			fmt.Printf("Saved face %d to: %s\n", i+1, faceFilename)

			// Compute perceptual hash for face recognition
			hash, err := goimagehash.PerceptionHash(faceImg)
			if err != nil {
				log.Printf("Failed to compute hash for face %d: %v", i+1, err)
			} else {
				fmt.Printf("Face %d hash: %s\n", i+1, hash.ToString())
				faceHashes = append(faceHashes, hash)
			}
		}
	}

	// Draw detections on image
	resultImg := DrawDetections(img, validDetections)

	// Save result
	if err := saveImage(resultImg, outputFile); err != nil {
		log.Fatalf("Failed to save image: %v", err)
	}

	fmt.Printf("Result saved to: %s\n", outputFile)

	return faceHashes
}

func compareFaces(hashes1, hashes2 []*goimagehash.ImageHash) {
	if len(hashes1) == 0 || len(hashes2) == 0 {
		fmt.Println("Cannot compare: one or both images have no faces")
		return
	}

	// Compare first faces
	hash1 := hashes1[0]
	hash2 := hashes2[0]

	distance, err := hash1.Distance(hash2)
	if err != nil {
		log.Printf("Failed to compute distance: %v", err)
		return
	}

	fmt.Printf("Hash distance between faces: %d\n", distance)

	// Threshold for similarity (lower distance means more similar)
	threshold := 10
	if distance <= threshold {
		fmt.Println("Faces are likely the same person")
	} else {
		fmt.Println("Faces are likely different people")
	}
}

func addFace(db *sql.DB, detector *FaceDetector, name, imageFile string) {
	fmt.Printf("Adding face for %s from %s\n", name, imageFile)

	// Process image to get hashes
	outputFile := strings.TrimSuffix(imageFile, filepath.Ext(imageFile)) + "_detected" + filepath.Ext(imageFile)
	hashes := processImage(detector, imageFile, outputFile)

	if len(hashes) == 0 {
		fmt.Println("No face found in image")
		return
	}

	hash := hashes[0] // Use the first face

	// Check if hash already exists
	var existingName string
	err := db.QueryRow("SELECT name FROM faces WHERE hash = ?", hash.ToString()).Scan(&existingName)
	if err == nil {
		fmt.Printf("Face already exists in database as: %s\n", existingName)
		return
	} else if err != sql.ErrNoRows {
		log.Printf("Failed to check existing face: %v", err)
		return
	}

	// Insert into database
	_, err = db.Exec("INSERT INTO faces (name, hash, image_path) VALUES (?, ?, ?)", name, hash.ToString(), imageFile)
	if err != nil {
		log.Printf("Failed to insert face: %v", err)
		return
	}

	fmt.Printf("Added face for %s to database\n", name)
}

func recognizeFace(db *sql.DB, detector *FaceDetector, imageFile string) {
	fmt.Printf("Recognizing face in %s\n", imageFile)

	// Process image to get hashes
	outputFile := strings.TrimSuffix(imageFile, filepath.Ext(imageFile)) + "_detected" + filepath.Ext(imageFile)
	hashes := processImage(detector, imageFile, outputFile)

	if len(hashes) == 0 {
		fmt.Println("No face found in image")
		return
	}

	// Cluster hashes by similarity to avoid multiple detections of same person
	var clusters [][]*goimagehash.ImageHash
	for _, hash := range hashes {
		found := false
		for i, cluster := range clusters {
			dist, _ := hash.Distance(cluster[0])
			if dist <= 5 { // Same person threshold
				clusters[i] = append(clusters[i], hash)
				found = true
				break
			}
		}
		if !found {
			clusters = append(clusters, []*goimagehash.ImageHash{hash})
		}
	}

	fmt.Printf("Clustered into %d person(s)\n", len(clusters))

	// Query all faces from database
	rows, err := db.Query("SELECT name, hash FROM faces")
	if err != nil {
		log.Printf("Failed to query faces: %v", err)
		return
	}
	defer rows.Close()

	var bestMatch string
	minDistance := 9999

	for _, cluster := range clusters {
		repHash := cluster[0] // Use first hash as representative
		for rows.Next() {
			var dbName, dbHashStr string
			err := rows.Scan(&dbName, &dbHashStr)
			if err != nil {
				log.Printf("Failed to scan row: %v", err)
				continue
			}

			// Parse hash string "p:hex"
			parts := strings.Split(dbHashStr, ":")
			if len(parts) != 2 {
				log.Printf("Invalid hash format: %s", dbHashStr)
				continue
			}
			hashValue, err := strconv.ParseUint(parts[1], 16, 64)
			if err != nil {
				log.Printf("Failed to parse hash value: %v", err)
				continue
			}

			dbHash := goimagehash.NewImageHash(hashValue, goimagehash.PHash)

			distance, err := repHash.Distance(dbHash)
			if err != nil {
				log.Printf("Failed to compute distance: %v", err)
				continue
			}

			if distance < minDistance {
				minDistance = distance
				bestMatch = dbName
			}
		}
	}

	if bestMatch != "" && minDistance <= 10 {
		fmt.Printf("Recognized as: %s (distance: %d)\n", bestMatch, minDistance)
		if minDistance <= 5 {
			fmt.Println("High confidence match")
		} else {
			fmt.Println("Low confidence match")
		}
	} else {
		fmt.Println("No match found in database")
	}
}

*/

package main

import (
	"io"
	"strings"
	"time"
)

// reader is an implementation of io.reader that draws the progress of
// reading some data.
type reader struct {
	// reader is the underlying reader to read from
	reader io.Reader

	// size is the total size of the data coming out of the reader.
	size int64

	// drawFunc is the callback to invoke to draw the progress bar. By
	// default, this will be drawTerminal(os.Stdout).
	//
	// drawInterval is the minimum time to wait between reads to update the
	// progress bar.
	drawFunc     drawFunc
	drawInterval time.Duration
	drawPrefix   string

	progress int64
	lastDraw time.Time
}

// Read reads from the underlying reader and invokes the drawFunc if
// appropriate. The drawFunc is executed when there is data that is
// read (progress is made) and at least drawInterval time has passed.
func (r *reader) Read(p []byte) (int, error) {
	// If we haven't drawn before, initialize the progress bar
	if r.lastDraw.IsZero() {
		r.initProgress()
	}

	// Read from the underlying source
	n, err := r.reader.Read(p)

	// Always increment the progress even if there was an error
	r.progress += int64(n)

	// If we don't have any errors, then draw the progress. If we are
	// at the end of the data, then finish the progress.
	if err == nil {
		// Only draw if we read data or we've never read data before (to
		// initialize the progress bar).
		if n > 0 {
			r.drawProgress()
		}
	}
	if err == io.EOF {
		r.finishProgress()
	}

	return n, err
}

func (r *reader) drawProgress() {
	// If we've drawn before, then make sure that the draw interval
	// has passed before we draw again.
	interval := r.drawInterval
	if interval == 0 {
		interval = time.Second
	}
	if !r.lastDraw.IsZero() {
		nextDraw := r.lastDraw.Add(interval)
		if time.Now().Before(nextDraw) {
			return
		}
	}

	// Draw
	if getDisplayTable() == strings.TrimPrefix(r.drawPrefix, "Downloading: ") {
		f := r.drawFunction()
		f(r.drawPrefix, r.progress, r.size)
	}

	// Record this draw so that we don't draw again really quickly
	r.lastDraw = time.Now()
}

func (r *reader) finishProgress() {
	// Only output the final draw if we drawed prior
	if !r.lastDraw.IsZero() {
		if getDisplayTable() == strings.TrimPrefix(r.drawPrefix, "Downloading: ") {
			f := r.drawFunction()
			f(r.drawPrefix, r.progress, r.size)

			// Blank out the line
			f(r.drawPrefix, -1, -1)
		}

		// Reset lastDraw so we don't finish again
		var zeroDraw time.Time
		r.lastDraw = zeroDraw
	}
}

func (r *reader) initProgress() {
	var zeroDraw time.Time
	r.lastDraw = zeroDraw
	r.drawProgress()
	r.lastDraw = zeroDraw
}

func (r *reader) drawFunction() drawFunc {
	return r.drawFunc
}

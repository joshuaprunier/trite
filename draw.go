package main

import (
	"fmt"
	"io"
	"strings"
)

// drawFunc is the callback type for drawing progress.
type drawFunc func(string, int64, int64) error

// drawTextFormatFunc is a callback used by drawFuncs that draw text in
// order to format the text into some more human friendly format.
type drawTextFormatFunc func(string, int64, int64) string

// drawTerminalf returns a drawFunc that draws a progress bar to an io.Writer
// that is formatted with the given formatting function.
func drawTerminalf(w io.Writer, f drawTextFormatFunc) drawFunc {
	var maxLength int

	return func(prefix string, progress, total int64) error {
		if progress == -1 && total == -1 {
			_, err := fmt.Fprintf(w, strings.Repeat(" ", maxLength))
			return err
		}

		// Make sure we pad it to the max length we've ever drawn so that
		// we don't have trailing characters.
		line := f(prefix, progress, total)
		if len(line) < maxLength {
			line = fmt.Sprintf(
				"%s%s",
				line,
				strings.Repeat(" ", maxLength-len(line)))
		}
		maxLength = len(line)

		_, err := fmt.Fprint(w, line+"\r")
		return err
	}
}

// drawTextFormatPercent is a drawTextFormatFunc that formats the progress
// into a percentage
func drawTextFormatPercent(prefix string, progress, total int64) string {
	return fmt.Sprintf("%s: %d%%", prefix, uint(float32(progress)/float32(total)*100))
}

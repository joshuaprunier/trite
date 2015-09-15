package ioprogress

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// DrawFunc is the callback type for drawing progress.
type DrawFunc func(string, int64, int64) error

// DrawTextFormatFunc is a callback used by DrawFuncs that draw text in
// order to format the text into some more human friendly format.
type DrawTextFormatFunc func(string, int64, int64) string

var defaultDrawFunc DrawFunc

func init() {
	defaultDrawFunc = DrawTerminal(os.Stdout)
}

// DrawTerminal returns a DrawFunc that draws a progress bar to an io.Writer
// that is assumed to be a terminal (and therefore respects carriage returns).
func DrawTerminal(w io.Writer) DrawFunc {
	return DrawTerminalf(w, func(prefix string, progress, total int64) string {
		return fmt.Sprintf("%s: %d/%d", prefix, progress, total)
	})
}

// DrawTerminalf returns a DrawFunc that draws a progress bar to an io.Writer
// that is formatted with the given formatting function.
func DrawTerminalf(w io.Writer, f DrawTextFormatFunc) DrawFunc {
	var maxLength int

	return func(prefix string, progress, total int64) error {
		if progress == -1 && total == -1 {
			fmt.Sprintf("%s%s", strings.Repeat(" ", maxLength))
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

// DrawTextFormatPercent is a DrawTextFormatFunc that formats the progress
// into a percentage
func DrawTextFormatPercent(prefix string, progress, total int64) string {
	return fmt.Sprintf("%s: %d%%", prefix, uint(float32(progress)/float32(total)*100))
}

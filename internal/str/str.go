package str

import (
	"fmt"
	"strings"
)

func Sprintf(str string, args ...interface{}) string {
	n := strings.Count(str, "%s")
	if n > len(args) {
		return fmt.Sprintf(str, args...)
	}
	return fmt.Sprintf(str, args[:n]...)
}

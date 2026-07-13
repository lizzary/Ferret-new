package extract

// Keep x/text encoding dependencies in this adapter file. Parser and registry
// code depend only on decodeGB18030, which keeps third-party API churn isolated.
import "golang.org/x/text/encoding/simplifiedchinese"

func decodeGB18030(raw []byte) ([]byte, error) {
	return simplifiedchinese.GB18030.NewDecoder().Bytes(raw)
}

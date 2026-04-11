package attestation

import "testing"

// Sample output taken from `snpguest 0.10 display report` on a GCP
// Confidential VM running AMD Milan (SEV-SNP). Used to pin the parser against
// a real-world shape, not a synthetic one.
const snpguestSample = `Attestation Report (1184 bytes):
Version: 2
Guest SVN: 7
Guest Policy (0x30000):
  ABI Major: 0
  ABI Minor: 0
  SMT Allowed: true
  Migrate MA: false
  Debug Allowed: false
  Single Socket Required: false
Family ID:
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
Image ID:
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
VMPL: 0
Signature Algorithm: 1
Current TCB:
  Boot Loader: 9
  TEE: 0
  SNP: 21
  Microcode: 213
Platform Info (0x3):
  SMT Enabled: true
  TSME Enabled: true
Author Key Encryption: false
Report Data:
aa bb cc dd 00 11 22 33 44 55 66 77 88 99 aa bb
cc dd ee ff 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
Measurement:
12 06 83 61 36 9c f9 17 9b b6 ac 08 57 2b 7e 15
ed 0b c8 ab b6 98 cb 04 d4 f5 84 f7 ff 51 2a 4c
20 81 c1 f5 b1 05 35 1d bd 45 c0 35 a7 d6 a3 a5
Host Data:
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
ID Key Digest:
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
Chip ID:
aa aa aa aa aa aa aa aa
`

func TestParseSnpguestReport(t *testing.T) {
	report := &SevSnpReport{}
	parseSnpguestReport(snpguestSample, report)

	wantMeasurement := "12068361369cf9179bb6ac08572b7e15" +
		"ed0bc8abb698cb04d4f584f7ff512a4c" +
		"2081c1f5b105351dbd45c035a7d6a3a5"

	if report.Measurement != wantMeasurement {
		t.Errorf("measurement mismatch:\n  got  = %q\n  want = %q", report.Measurement, wantMeasurement)
	}

	wantReportData := "aabbccdd00112233445566778899aabb" +
		"ccddeeff000000000000000000000000" +
		"00000000000000000000000000000000" +
		"00000000000000000000000000000000"
	if report.ReportData != wantReportData {
		t.Errorf("report_data mismatch:\n  got  = %q\n  want = %q", report.ReportData, wantReportData)
	}
}

func TestParseSnpguestReport_EmptyInput(t *testing.T) {
	report := &SevSnpReport{}
	parseSnpguestReport("", report)
	if report.Measurement != "" || report.ReportData != "" {
		t.Errorf("empty input should leave all fields empty, got %+v", report)
	}
}

func TestParseSnpguestReport_OnlyFieldHeadersNoHex(t *testing.T) {
	input := "Measurement:\nReport Data:\nChip ID:\n"
	report := &SevSnpReport{}
	parseSnpguestReport(input, report)
	if report.Measurement != "" || report.ReportData != "" {
		t.Errorf("headers without hex lines should leave fields empty, got %+v", report)
	}
}

func TestParseSnpguestReport_SingleLineMeasurement(t *testing.T) {
	// Some snpguest versions collapse short fields onto one hex line.
	input := "Measurement:\n12 06 83 61 36 9c f9 17\n"
	report := &SevSnpReport{}
	parseSnpguestReport(input, report)
	want := "12068361369cf917"
	if report.Measurement != want {
		t.Errorf("single-line measurement:\n  got  = %q\n  want = %q", report.Measurement, want)
	}
}

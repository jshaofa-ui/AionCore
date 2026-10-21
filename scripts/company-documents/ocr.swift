import Foundation
import Vision
import ImageIO

let url = URL(fileURLWithPath: CommandLine.arguments[1])
let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.recognitionLanguages = ["zh-Hans", "en-US"]
request.usesLanguageCorrection = true
let handler = VNImageRequestHandler(url: url)
do {
    try handler.perform([request])
    for result in request.results ?? [] {
        if let candidate = result.topCandidates(1).first { print(candidate.string) }
    }
} catch {
    fputs("OCR failed\n", stderr)
    exit(1)
}

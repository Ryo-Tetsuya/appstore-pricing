import AppKit
import Foundation

let outputPath = CommandLine.arguments.dropFirst().first ?? "target/macos/AppIcon.iconset"
let outputURL = URL(fileURLWithPath: outputPath)
try FileManager.default.createDirectory(at: outputURL, withIntermediateDirectories: true)

struct RGBA {
    let red: CGFloat
    let green: CGFloat
    let blue: CGFloat
    let alpha: CGFloat

    init(_ red: CGFloat, _ green: CGFloat, _ blue: CGFloat, _ alpha: CGFloat = 1.0) {
        self.red = red / 255.0
        self.green = green / 255.0
        self.blue = blue / 255.0
        self.alpha = alpha
    }

    var cgColor: CGColor {
        CGColor(red: red, green: green, blue: blue, alpha: alpha)
    }
}

struct IconSpec {
    let points: Int
    let scale: Int
    let pixels: Int

    var filename: String {
        scale == 2
            ? "icon_\(points)x\(points)@2x.png"
            : "icon_\(points)x\(points).png"
    }
}

let specs = [
    IconSpec(points: 16, scale: 1, pixels: 16),
    IconSpec(points: 16, scale: 2, pixels: 32),
    IconSpec(points: 32, scale: 1, pixels: 32),
    IconSpec(points: 32, scale: 2, pixels: 64),
    IconSpec(points: 128, scale: 1, pixels: 128),
    IconSpec(points: 128, scale: 2, pixels: 256),
    IconSpec(points: 256, scale: 1, pixels: 256),
    IconSpec(points: 256, scale: 2, pixels: 512),
    IconSpec(points: 512, scale: 1, pixels: 512),
    IconSpec(points: 512, scale: 2, pixels: 1024),
]

func fillRoundedRect(_ context: CGContext, _ rect: CGRect, _ radius: CGFloat, _ color: RGBA) {
    context.setFillColor(color.cgColor)
    context.addPath(CGPath(roundedRect: rect, cornerWidth: radius, cornerHeight: radius, transform: nil))
    context.fillPath()
}

func drawLine(_ context: CGContext, from start: CGPoint, to end: CGPoint, color: RGBA) {
    context.setStrokeColor(color.cgColor)
    context.setLineWidth(4)
    context.setLineCap(.round)
    context.setLineJoin(.round)
    context.move(to: start)
    context.addLine(to: end)
    context.strokePath()
}

func drawNode(_ context: CGContext, at center: CGPoint) {
    let outer = CGRect(x: center.x - 4.8, y: center.y - 4.8, width: 9.6, height: 9.6)
    context.setFillColor(RGBA(247, 251, 255).cgColor)
    context.fillEllipse(in: outer)
    context.setStrokeColor(RGBA(0, 102, 204).cgColor)
    context.setLineWidth(3)
    context.strokeEllipse(in: outer)
}

func renderIcon(pixels: Int) throws -> Data {
    let colorSpace = CGColorSpaceCreateDeviceRGB()
    guard let context = CGContext(
        data: nil,
        width: pixels,
        height: pixels,
        bitsPerComponent: 8,
        bytesPerRow: pixels * 4,
        space: colorSpace,
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    ) else {
        throw NSError(domain: "AppIcon", code: 1, userInfo: [NSLocalizedDescriptionKey: "Unable to create bitmap context"])
    }

    let scale = CGFloat(pixels) / 128.0
    context.scaleBy(x: scale, y: scale)
    context.translateBy(x: 0, y: 128)
    context.scaleBy(x: 1, y: -1)

    let background = CGPath(roundedRect: CGRect(x: 6, y: 6, width: 116, height: 116), cornerWidth: 28, cornerHeight: 28, transform: nil)
    context.saveGState()
    context.addPath(background)
    context.clip()
    let gradient = CGGradient(
        colorsSpace: colorSpace,
        colors: [RGBA(6, 20, 44).cgColor, RGBA(0, 102, 204).cgColor] as CFArray,
        locations: [0.0, 1.0]
    )!
    context.drawLinearGradient(
        gradient,
        start: CGPoint(x: 18, y: 120),
        end: CGPoint(x: 111, y: 8),
        options: []
    )
    context.restoreGState()

    fillRoundedRect(context, CGRect(x: 22, y: 24, width: 84, height: 80), 15, RGBA(248, 252, 255, 0.94))
    fillRoundedRect(context, CGRect(x: 32, y: 38, width: 64, height: 11), 4, RGBA(0, 102, 204, 0.90))
    fillRoundedRect(context, CGRect(x: 34, y: 60, width: 32, height: 5), 2.5, RGBA(103, 117, 137, 0.62))
    fillRoundedRect(context, CGRect(x: 34, y: 74, width: 25, height: 5), 2.5, RGBA(103, 117, 137, 0.62))
    fillRoundedRect(context, CGRect(x: 34, y: 88, width: 36, height: 5), 2.5, RGBA(103, 117, 137, 0.62))
    fillRoundedRect(context, CGRect(x: 76, y: 59, width: 19, height: 6), 3, RGBA(19, 188, 235, 0.90))
    fillRoundedRect(context, CGRect(x: 74, y: 73, width: 21, height: 6), 3, RGBA(0, 102, 204, 0.88))
    fillRoundedRect(context, CGRect(x: 72, y: 87, width: 23, height: 6), 3, RGBA(33, 212, 177, 0.90))

    drawLine(context, from: CGPoint(x: 33, y: 95), to: CGPoint(x: 52, y: 80), color: RGBA(0, 102, 204, 0.90))
    drawLine(context, from: CGPoint(x: 52, y: 80), to: CGPoint(x: 72, y: 86), color: RGBA(0, 102, 204, 0.90))
    drawLine(context, from: CGPoint(x: 72, y: 86), to: CGPoint(x: 96, y: 58), color: RGBA(33, 212, 177, 0.92))
    for point in [
        CGPoint(x: 33, y: 95),
        CGPoint(x: 52, y: 80),
        CGPoint(x: 72, y: 86),
        CGPoint(x: 96, y: 58),
    ] {
        drawNode(context, at: point)
    }

    guard let image = context.makeImage() else {
        throw NSError(domain: "AppIcon", code: 2, userInfo: [NSLocalizedDescriptionKey: "Unable to create CGImage"])
    }
    let rep = NSBitmapImageRep(cgImage: image)
    guard let png = rep.representation(using: .png, properties: [:]) else {
        throw NSError(domain: "AppIcon", code: 3, userInfo: [NSLocalizedDescriptionKey: "Unable to encode PNG"])
    }
    return png
}

var pngByPixelSize: [Int: Data] = [:]

for spec in specs {
    let png: Data
    if let existing = pngByPixelSize[spec.pixels] {
        png = existing
    } else {
        png = try renderIcon(pixels: spec.pixels)
    }
    pngByPixelSize[spec.pixels] = png
    try png.write(to: outputURL.appendingPathComponent(spec.filename), options: .atomic)
}

func appendAscii(_ string: String, to data: inout Data) {
    data.append(contentsOf: string.utf8)
}

func appendBigEndianUInt32(_ value: UInt32, to data: inout Data) {
    var bigEndian = value.bigEndian
    withUnsafeBytes(of: &bigEndian) { bytes in
        data.append(contentsOf: bytes)
    }
}

if CommandLine.arguments.count > 2 {
    let icnsURL = URL(fileURLWithPath: CommandLine.arguments[2])
    let chunks: [(String, Int)] = [
        ("icp4", 16),
        ("icp5", 32),
        ("icp6", 64),
        ("ic07", 128),
        ("ic08", 256),
        ("ic09", 512),
        ("ic10", 1024),
    ]

    var body = Data()
    for (type, pixels) in chunks {
        let png: Data
        if let existing = pngByPixelSize[pixels] {
            png = existing
        } else {
            png = try renderIcon(pixels: pixels)
        }
        appendAscii(type, to: &body)
        appendBigEndianUInt32(UInt32(png.count + 8), to: &body)
        body.append(png)
    }

    var icns = Data()
    appendAscii("icns", to: &icns)
    appendBigEndianUInt32(UInt32(body.count + 8), to: &icns)
    icns.append(body)
    try icns.write(to: icnsURL, options: .atomic)
}

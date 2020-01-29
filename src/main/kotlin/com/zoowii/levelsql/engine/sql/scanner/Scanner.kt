package com.zoowii.levelsql.engine.sql.scanner

import com.google.common.base.Strings
import com.zoowii.levelsql.engine.exceptions.SqlParseException
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.reservedCount
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkEOS
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkGL
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkGe
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkInt
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkLe
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkName
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkNe
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkNumber
import com.zoowii.levelsql.engine.sql.scanner.TokenTypes.tkString
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.lang.Character.isDigit
import java.lang.Character.isLetter
import java.math.BigDecimal
import java.util.*

fun isNewLine(t: Rune): Boolean {
    return t == '\n'.toInt() || t == '\r'.toInt()
}

fun isDecimal(t: Rune): Boolean {
    return t in '0'.toInt()..'9'.toInt()
}

val escapes: Map<Rune, Rune> = hashMapOf(
        Pair('b'.toInt(), '\b'.toInt()),
        Pair('n'.toInt(), '\n'.toInt()),
        Pair('r'.toInt(), '\r'.toInt()),
        Pair('t'.toInt(), '\t'.toInt()),
        Pair('\\'.toInt(), '\\'.toInt()),
        Pair('"'.toInt(), '"'.toInt()),
        Pair('\''.toInt(), '\''.toInt())
)

class Scanner(private val source: String, private val reader: InputStream) {
    private var scanningToken = Token(0)
    private val buffer = ByteArrayOutputStream()
    private var current: Rune = 0
    private var lineNumber: Int = 0
    private var lastLine: Int = 0
    private var lookAheadToken = Token(tkEOS)

    fun assert(cond: Boolean) {
        if (!cond) {
            throw SqlParseException("parse sql error")
        }
    }

    fun syntaxError(msg: String) {
        scanError(msg, scanningToken.t)
    }

    fun errorExpected(t: Rune) {
        syntaxError(tokenToString(t) + " expected")
    }

    fun numberError() {
        scanError("invalid number", tkNumber)
    }

    fun intError() {
        scanError("invalid int", tkInt)
    }

    fun scanError(msg: String, token: Rune) {
        val s: String
        if (token != 0) {
            s = "$source:$lineNumber: $msg near ${tokenToString(token)}"
        } else {
            s = "$source:$lineNumber: $msg"
        }
        throw SqlParseException(s)
    }

    fun tokenToString(t: Rune): String {
        when {
            t == tkName || t == tkString -> {
                return scanningToken.s
            }
            t == tkInt -> {
                return "${scanningToken.i}"
            }
            t == tkNumber -> {
                return "${scanningToken.n}"
            }
            t < firstReserved -> {
                return t.toString() // TODO: to printable string
            }
            t < tkEOS -> {
                return "'${tokens[t - firstReserved]}'"
            }
        }
        return tokens[t - firstReserved]
    }

    fun incrementLineNumber() {
        val old = current
        assert(isNewLine(old))
        advance()
        if (isNewLine(current) && current != old) {
            advance()
        }
        lineNumber++
        if (lineNumber >= maxInt) {
            syntaxError("chunk has too many lines")
        }
    }

    fun currentToken(): Token {
        return scanningToken
    }

    fun advance() {
        current = if (reader.available() <= 0) {
            endOfStream
        } else {
            reader.read()
        }
    }

    fun saveAndAdvance() {
        save(current)
        advance()
    }

    fun advanceAndSave(c: Rune) {
        advance()
        save(c)
    }

    fun save(c: Rune) {
        buffer.write(c)
    }

    fun checkNext(str: String): Boolean {
        if (current == 0 || !str.contains(current.toChar())) {
            return false
        }
        saveAndAdvance()
        return true
    }

    fun skipSeparator(): Int {
        val c = current
        assert(c == '['.toInt() || c == ']'.toInt())
        saveAndAdvance()
        var i = 0
        while (current == '='.toInt()) {
            saveAndAdvance()
            i++
        }
        if (current == c) {
            return i
        }
        return -i - 1
    }

    fun readDigits(): Rune {
        var c = current
        while (isDecimal(c)) {
            saveAndAdvance()
            c = current
        }
        return c
    }

    fun isHexadecimal(c: Rune): Boolean {
        return '0'.toInt() <= c && c <= '9'.toInt()
                || 'a'.toInt() <= c && c <= 'f'.toInt()
                || 'A'.toInt() <= c && c <= 'F'.toInt()
    }

    fun readHexNumber(x: Double): Triple<Double, Rune, Int> {
        var c = current
        var n = x
        var i = 0
        if (!isHexadecimal(c)) {
            return Triple(n, c, i)
        }
        while (true) {
            when {
                '0'.toInt() <= c && c <= '9'.toInt() -> {
                    c -= '0'.toInt()
                }
                'a'.toInt() <= c && c <= 'f'.toInt() -> {
                    c = c - 'a'.toInt() + 10
                }
                'A'.toInt() <= c && c <= 'F'.toInt() -> {
                    c = c - 'A'.toInt() + 10
                }
                else -> {
                    return Triple(n, c, i)
                }
            }
            advance()
            c = current
            n = n * 16.0 + c.toDouble()
            i++
        }
    }

    fun readNumber(): Token {
        val base10 = 10
        var c = current
        assert(isDecimal(c))
        saveAndAdvance()
        var isNumber = false
        if (c == '0'.toInt() && checkNext("Xx")) { // hexadecimal
            val prefix = buffer.toString("UTF-8")
            assert(prefix == "0x" || prefix == "0X")
            buffer.reset()
            var exponent: Int = 0
            val tmp1 = readHexNumber(0.0)
            var fraction = tmp1.first
            c = tmp1.second
            var i = tmp1.third
            if (c == '.'.toInt()) {
                isNumber = true
                advance()
                val tmp2 = readHexNumber(fraction)
                fraction = tmp2.first
                c = tmp2.second
                exponent = tmp2.third
            }
            if (i == 0 && exponent == 0) {
                numberError()
            }
            exponent *= -4
            if (c == 'p'.toInt() || c == 'P'.toInt()) {
                advance()
                var negativeExponent = false
                c = current
                if (c == '+'.toInt() || c == '-'.toInt()) {
                    negativeExponent = c == '-'.toInt()
                    advance()
                }
                if (!isDecimal(current)) {
                    numberError()
                }
                readDigits()
                try {
                    val e = java.lang.Long.parseLong(buffer.toString("UTF-8"), base10)
                    if (negativeExponent) {
                        exponent += (-e).toInt()
                        isNumber = true
                    } else {
                        exponent += e.toInt()
                    }
                } catch (e: Exception) {
                    numberError()
                } finally {
                    buffer.reset()
                }
            }
            if (isNumber) {
                val n = BigDecimal(fraction).multiply(BigDecimal(2).pow(exponent))
                return Token(tkNumber, null, n, "")
            } else {
                val intValue = (fraction * (Math.pow(2.0, exponent.toDouble()).toLong())).toLong()
                return Token(tkInt, intValue, null, "")
            }

        }
        c = readDigits()
        if (c == '.'.toInt()) {
            isNumber = true
            saveAndAdvance()
            c = readDigits()
        }
        if (c == 'e'.toInt() || c == 'E'.toInt()) {
            saveAndAdvance()
            c = current
            if (c == '+'.toInt() || c == '-'.toInt()) {
                saveAndAdvance()
            }
            readDigits()
        }
        var str = buffer.toString("UTF-8")
        if (str.startsWith("0")) {
            str = str.trimStart('0')
            if (str == "" || !isDecimal((str[0]).toInt())) {
                str = "0" + str
            }
        }
        var f = BigDecimal.ZERO
        try {
            f = BigDecimal(str)
        } catch (e: Exception) {
            numberError()
        }
        buffer.reset()
        if (str.length > 0 && str[0] == '.') {
            isNumber = true
        }
        if (isNumber) {
            return Token(tkNumber, null, f, "")
        } else {
            return Token(tkInt, f.longValueExact(), null, "")
        }
    }

    fun escapeError(c: Array<Rune>, message: String) {
        buffer.reset()
        save('\\'.toInt())
        for (r in c) {
            if (r == endOfStream) {
                break
            }
            save(r)
        }
        scanError(message, tkString)
    }

    fun readHexEscape(): Rune {
        advance()
        var i = 1
        var c = current
        var b: Array<Rune> = arrayOf('x'.toInt(), 'x'.toInt(), 'x'.toInt())
        var r: Rune = 0
        while (i < b.size) {
            b[i] = c
            when {
                '0'.toInt() <= c && c <= '9'.toInt() -> {
                    c = c - '0'.toInt()
                }
                'a'.toInt() <= c && c <= 'f'.toInt() -> {
                    c = c - 'a'.toInt() + 10
                }
                'A'.toInt() <= c && c <= 'F'.toInt() -> {
                    c = c - 'A'.toInt() + 10
                }
                else -> {
                    val subB: Array<Rune> = Arrays.copyOf(b, i + 1)
                    escapeError(subB, "hexadecimal digit expected")
                }
            }
            advance()

            i++
            c = current
            r = r.shl(4) + c
        }
        return r
    }

    fun readDecimalEscape(): Rune {
        val b = Array<Rune>(3, { 0 })
        var c = current
        var i = 0
        var r: Rune = 0
        while (i < b.size && isDecimal(c)) {
            b[i] = c
            r = 10 * r + c - '0'.toInt()
            advance()

            i++
            c = current
        }

        if (r > maxUint8) {
            escapeError(Arrays.copyOf(b, b.size), "decimal escape too large")
        }
        return r
    }

    fun readString(): Token {
        val delimiter = current
        saveAndAdvance()
        while (current != delimiter) {
            when (current) {
                endOfStream -> {
                    scanError("unfinished string", tkEOS)
                }
                '\n'.toInt(), '\r'.toInt() -> {
                    scanError("unfinished string", tkString)
                }
                '\\'.toInt() -> {
                    advance()
                    var c = current
                    val ok = escapes.containsKey(c)
                    val esc = escapes.getOrDefault(c, 0)
                    when {
                        ok -> {
                            advanceAndSave(esc)
                        }
                        isNewLine(c) -> {
                            incrementLineNumber()
                            save('\n'.toInt())
                        }
                        c == endOfStream -> {
                            // do nothing
                        }
                        c == 'x'.toInt() -> {
                            save(readHexEscape())
                        }
                        c == 'z'.toInt() -> {
                            advance()

                            while (isSpace(current)) {
                                if (isNewLine(current)) {
                                    incrementLineNumber()
                                } else {
                                    advance()
                                }
                            }
                        }
                        else -> {
                            if (!isDecimal(c)) {
                                escapeError(arrayOf<Rune>(c), "invalid escape sequence")
                            }
                            save(readDecimalEscape())
                        }
                    }
                }
                else -> {
                    saveAndAdvance()
                }
            }
        }
        saveAndAdvance()
        val str = buffer.toString("UTF-8")
        buffer.reset()
        return Token(tkString, null, null, str.substring(1, str.length - 1))
    }

    fun isReserved(s: String): Boolean {
        for (reserved in Arrays.copyOf(tokens, reservedCount)) {
            if (s == reserved) {
                return true
            }
        }
        return false
    }

    fun reservedOrName(): Token {
        val str = buffer.toString("UTF-8")
        buffer.reset()
        for ((i, reserved) in Arrays.copyOf(tokens, reservedCount).withIndex()) {
            if (str == reserved) {
                return Token(i + firstReserved, null, null, reserved)
            }
        }
        return Token(tkName, null, null, str)
    }

    fun scan(): Token {
        val comment = true
        val str = false
        loop@ while (true) {
            var c = current
            when (c) {
                '\n'.toInt(), '\r'.toInt() -> {
                    incrementLineNumber()
                }
                ' '.toInt(), '\t'.toInt() -> {
                    advance()
                }
                '-'.toInt() -> {
                    advance()
                    if (current != '-'.toInt()) {
                        return Token('-'.toInt())
                    }
                    advance()
                    while (!isNewLine(current) && current != endOfStream) {
                        advance()
                    }
                }
                '['.toInt() -> {
                    advance()
                    return Token('['.toInt())
                }
                '='.toInt() -> {
                    advance()
                    return Token('='.toInt())
                }
                '<'.toInt() -> {
                    advance()
                    if (current == '='.toInt()) {
                        advance()
                        return Token(tkLe)
                    }
                    if (current == '>'.toInt()) {
                        advance()
                        return Token(tkGL)
                    }
                    return Token('<'.toInt())
                }
                '/'.toInt() -> {
                    advance()
                    if (current != '/'.toInt()) {
                        return Token('/'.toInt())
                    }
                    // 单行注释
                    while (!isNewLine(current) && current != endOfStream) {
                        advance()
                    }
                }
                '>'.toInt() -> {
                    advance()
                    if (current == '='.toInt()) {
                        advance()
                        return Token(tkGe)
                    }
                    return Token('>'.toInt())
                }
                '!'.toInt() -> {
                    advance()
                    if (current != '='.toInt()) {
                        return Token('!'.toInt())
                    }
                    advance()
                    return Token(tkNe)
                }
                ':'.toInt() -> {
                    advance()
                    return Token(':'.toInt())
                }
                '"'.toInt(), '\''.toInt() -> {
                    return readString()
                }
                endOfStream -> {
                    return Token(tkEOS)
                }
                '.'.toInt() -> {
                    saveAndAdvance()
                    if (!isDigit(current)) {
                        buffer.reset()
                        return Token('.'.toInt())
                    } else {
                        return readNumber()
                    }
                }
                0 -> {
                    advance()
                }
                else -> {
                    if (isDigit(c)) {
                        return readNumber()
                    } else if (c == '_'.toInt() || isLetter(c)) {
                        while (c == '_'.toInt() || isLetter(c) || isDigit(c)) {
                            saveAndAdvance()
                            c = current
                        }
                        return reservedOrName()
                    }
                    advance()
                    return Token(c)
                }
            }
        }
        throw SqlParseException("unreachable")
    }

    fun next() {
        lastLine = lineNumber
        if (lookAheadToken.t != tkEOS) {
            scanningToken = lookAheadToken
            lookAheadToken.t = tkEOS
        } else {
            scanningToken = scan()
        }
    }

    fun lookAhead(): Rune {
        assert(lookAheadToken.t == tkEOS)
        lookAheadToken = scan()
        return lookAheadToken.t
    }

    fun testNext(t: Rune): Boolean {
        var r = scanningToken.t == t
        if (r) {
            next()
        }
        return r
    }

    fun check(t: Rune) {
        if (scanningToken.t != t) {
            errorExpected(t)
        }
    }

    fun checkMatch(what: Rune, who: Rune, where: Int) {
        if (!testNext(what)) {
            if (where == lineNumber) {
                errorExpected(what)
            } else {
                syntaxError("${tokenToString(what)} expected (to close ${tokenToString(who)} at Line ${where})")
            }
        }
    }
}
// Package format 美化 Atlas 生成的迁移 SQL。独立成子包是为了让迁移生成工具
// （migrations/gen）在内嵌的 migrations 包还编译不过时（尚无任何迁移文件）
// 也能使用它。
package format

import (
	"bytes"
	"slices"
	"strings"

	"ariga.io/atlas/sql/migrate"
)

// Formatter 用来生成迁移文件：内容取 Atlas 的缺省格式，再经 SQL 美化。
type Formatter struct{}

// Format implements migrate.Formatter.
func (Formatter) Format(plan *migrate.Plan) ([]migrate.File, error) {
	files, err := migrate.DefaultFormatter.Format(plan)
	if err != nil {
		return nil, err
	}
	for i, f := range files {
		if lf, ok := f.(*migrate.LocalFile); ok {
			files[i] = migrate.NewLocalFile(lf.Name(), SQL(lf.Bytes()))
		}
	}
	return files, nil
}

const (
	// maxWidth 是单条语句不折行的宽度上限，超出后在顶层子句关键字前折行。
	maxWidth = 100
	// indent 是折行与表元素行的缩进。
	indent = "    "
)

// clauseKeywords 是折行点：出现在括号外时在其前面换行。折行只插入空白，
// 误判（如未加引号的同名列名）也不影响语句的语义。
var clauseKeywords = map[string]bool{
	"ON": true, "WHERE": true, "TO": true, "FROM": true, "USING": true, "SET": true,
	"ADD": true, "DROP": true, "ALTER": true, "REFERENCES": true, "RETURNING": true,
	"INCLUDING": true, "EXCLUDE": true, "TABLESPACE": true, "WITH": true,
}

// SQL 美化 Atlas 生成的迁移文件内容。只调整空白：
//   - 语句之间空一行，语句前的注释行与其成组；
//   - CREATE TABLE 的表元素（列、约束）各占一行，列名对齐；
//   - 超过 maxWidth 的语句在顶层子句关键字前折行。
//
// 唯一的例外是列定义末尾冗余的 NULL 标记（可空列缺省即可空），予以省略。
// 语句正文一律按原文字节切片、不重新拼接，词法无法识别的内容（多行语句、
// 语句内注释、引号不配对）原样保留。
func SQL(src []byte) []byte {
	var (
		out        bytes.Buffer
		stmt       []string // 累积中的语句；Atlas 缺省格式下一行即一条
		afterStmt  bool     // 上一条输出是语句：下一个语句组之前空一行
		afterBlank bool     // 上一条输出是空行：不叠加连续空行
	)
	flush := func() {
		if len(stmt) == 0 {
			return
		}
		out.WriteString(formatStmt(strings.Join(stmt, "\n")))
		out.WriteByte('\n')
		stmt, afterStmt, afterBlank = nil, true, false
	}
	for _, raw := range strings.Split(strings.TrimSuffix(string(src), "\n"), "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case line == "":
			// 只保留文件头指令块之后的那一个空行。
			if out.Len() > 0 && !afterStmt && !afterBlank {
				out.WriteByte('\n')
				afterBlank = true
			}
		case len(stmt) > 0:
			// 语句尚未结束：后续各行（含注释行）都属于它
			stmt = append(stmt, raw)
			if stmtComplete(strings.Join(stmt, "\n")) {
				flush()
			}
		case strings.HasPrefix(line, "--"):
			flush()
			if afterStmt {
				out.WriteByte('\n')
			}
			afterStmt, afterBlank = false, false
			out.WriteString(line)
			out.WriteByte('\n')
		default:
			stmt = append(stmt, raw)
			if stmtComplete(raw) {
				flush()
			}
		}
	}
	flush()
	return out.Bytes()
}

// stmtComplete 判断累积的文本是否已是一条完整的语句（括号与引号之外出现了
// 分号）。
func stmtComplete(text string) bool {
	toks, ok := tokenize(text)
	if !ok {
		// 词法无法识别（如语句内注释）：退化为按行尾分号判断，语句不被拆开
		return strings.HasSuffix(text, ";")
	}
	return len(toks) > 0 && toks[len(toks)-1].text == ";"
}

// formatStmt 美化一条以分号结尾的单行语句；不在处理范围内的原样返回。
func formatStmt(text string) string {
	if strings.Contains(strings.TrimSuffix(text, "\n"), "\n") {
		return text // 多行语句不是 Atlas 的缺省形态，不处理
	}
	toks, ok := tokenize(text)
	if !ok || len(toks) < 2 || toks[len(toks)-1].text != ";" {
		return text
	}
	// text 截到分号之前作为语句正文，分号由格式化函数统一补回
	text, body := text[:toks[len(toks)-1].start], toks[:len(toks)-1]
	if isWord(body[0], "CREATE") && len(body) > 1 && isWord(body[1], "TABLE") {
		return formatCreateTable(text, body)
	}
	return formatClauses(text, body)
}

// formatCreateTable 把建表语句排成每行一个表元素、列名对齐的形态。text 是
// 不含结尾分号的语句正文。
func formatCreateTable(text string, toks []tok) string {
	open := slices.IndexFunc(toks, func(t tok) bool { return t.text == "(" })
	if open < 0 {
		return text + ";" // 不在建表语句的常规形态内：保持原样
	}
	close := -1
	for depth, i := 0, open; i < len(toks) && close < 0; i++ {
		switch toks[i].text {
		case "(":
			depth++
		case ")":
			if depth--; depth == 0 {
				close = i
			}
		}
	}
	if close < 0 {
		return text + ";" // 不在建表语句的常规形态内：保持原样
	}

	type elem struct {
		name, body string
		column     bool
	}
	var (
		elems   []elem
		maxName int
	)
	// 按括号外的逗号切出表元素，逐个按原文字节取出
	for depth, start, i := 0, open+1, open+1; i <= close; i++ {
		if i == close || (depth == 0 && toks[i].text == ",") {
			if i > start {
				var e elem
				if e.name, e.body, e.column = column(text, toks[start:i]); e.column {
					maxName = max(maxName, len(e.name))
				} else {
					e.body = strings.TrimSpace(text[toks[start].start:toks[i-1].end])
				}
				elems = append(elems, e)
			}
			start = i + 1
			continue
		}
		switch toks[i].text {
		case "(":
			depth++
		case ")":
			depth--
		}
	}
	if len(elems) == 0 {
		return text + ";" // 不在建表语句的常规形态内：保持原样
	}

	var b strings.Builder
	b.WriteString(strings.TrimSpace(text[:toks[open].start]))
	b.WriteString(" (\n")
	for i, e := range elems {
		b.WriteString(indent)
		if e.column {
			b.WriteString(e.name)
			b.WriteString(strings.Repeat(" ", maxName-len(e.name)+1))
		}
		b.WriteString(e.body)
		if i < len(elems)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString(")")
	if tail := strings.TrimSpace(text[toks[close].end:]); tail != "" {
		b.WriteString(" " + tail)
	}
	b.WriteString(";")
	return b.String()
}

// column 拆出列定义的列名与定义体；不是列定义（PRIMARY KEY、CONSTRAINT 等）
// 时 ok 为 false。定义体末尾冗余的 NULL 标记被省略：Atlas 总在列定义最后输出
// 可空标记（NULL 或 NOT NULL），其中 NULL 是列的缺省行为。前面是 NOT（NOT
// NULL）或 DEFAULT（默认值恰为 NULL）时，这个 NULL 另有含义，不能动。
func column(text string, toks []tok) (name, body string, ok bool) {
	if len(toks) == 0 || toks[0].kind != tokQuoted {
		return "", "", false
	}
	end := toks[len(toks)-1].end
	if n := len(toks); n >= 3 && isWord(toks[n-1], "NULL") && !isWord(toks[n-2], "NOT") && !isWord(toks[n-2], "DEFAULT") {
		end = toks[n-2].end
	}
	return text[toks[0].start:toks[0].end], strings.TrimSpace(text[toks[0].end:end]), true
}

// formatClauses 在宽度允许时保持单行，超宽则在括号外的子句关键字前折行。
// text 是不含结尾分号的语句正文。
func formatClauses(text string, toks []tok) string {
	if len(text) < maxWidth {
		return text + ";" // 加上分号不超过 maxWidth
	}
	var (
		b     strings.Builder
		start int
		depth int
		cut   bool
	)
	for i, t := range toks {
		switch t.text {
		case "(":
			depth++
		case ")":
			depth--
		}
		if i > 0 && depth == 0 && t.kind == tokWord && clauseKeywords[strings.ToUpper(t.text)] {
			b.WriteString(strings.TrimRight(text[start:t.start], " "))
			b.WriteString("\n" + indent)
			start, cut = t.start, true
		}
	}
	if !cut {
		return text + ";" // 没有折行点：保持原样
	}
	b.WriteString(strings.TrimRight(text[start:], " "))
	b.WriteString(";")
	return b.String()
}

// tok 是 SQL 文本的一个词法单元；start/end 是它在原文中的字节区间。
type tok struct {
	text       string
	start, end int
	kind       tokKind
}

type tokKind uint8

const (
	tokWord    tokKind = iota // 未加引号的字面量：标识符、关键字
	tokQuoted                 // 双引号标识符
	tokLiteral                // 字符串字面量（含 E'' 与美元引号）
	tokNumber                 // 数值与 $n 参数
	tokPunct                  // 标点、括号与运算符
)

// tokenize 切分 PostgreSQL 方言的 SQL 文本，空白被丢弃。语句内出现注释、引号
// 或美元引号不配对时返回 ok=false，调用方应原样保留这段文本。
func tokenize(s string) (toks []tok, ok bool) {
	i := 0
	emit := func(j int, kind tokKind) {
		toks = append(toks, tok{s[i:j], i, j, kind})
		i = j
	}
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case strings.HasPrefix(s[i:], "--"), strings.HasPrefix(s[i:], "/*"):
			return nil, false // 语句内注释
		case c == '"':
			j := i + 1
			for {
				k := strings.IndexByte(s[j:], '"')
				if k < 0 {
					return nil, false
				}
				j += k + 1
				if j >= len(s) || s[j] != '"' {
					break // "" 是转义，不是结束
				}
				j++
			}
			emit(j, tokQuoted)
		case c == '\'':
			end, found := scanString(s, i, false)
			if !found {
				return nil, false
			}
			emit(end, tokLiteral)
		case isIdentStart(c):
			j := i + 1
			for j < len(s) && isIdentPart(s[j]) {
				j++
			}
			// E'...' 是转义字符串，反斜杠在其中有转义含义
			if j-i == 1 && (c == 'E' || c == 'e') && j < len(s) && s[j] == '\'' {
				end, found := scanString(s, i, true)
				if !found {
					return nil, false
				}
				emit(end, tokLiteral)
				break
			}
			emit(j, tokWord)
		case c == '$' && i+1 < len(s) && isDigit(s[i+1]):
			j := i + 2
			for j < len(s) && isDigit(s[j]) {
				j++
			}
			emit(j, tokNumber) // $n 参数
		case isDigit(c):
			j := i + 1
			for j < len(s) && (isIdentPart(s[j]) || (s[j] == '.' && j+1 < len(s) && isDigit(s[j+1]))) {
				j++
			}
			emit(j, tokNumber)
		case strings.HasPrefix(s[i:], "::"):
			emit(i+2, tokPunct)
		default:
			j := i + 1
			if end, found := scanDollar(s, i); found {
				// 美元引号字符串：$$...$$ 或 $tag$...$tag$
				emit(end, tokLiteral)
				break
			}
			for isOperator(c) && j < len(s) && isOperator(s[j]) {
				j++
			}
			emit(j, tokPunct)
		}
	}
	return toks, true
}

// scanString 返回字符串字面量的结束位置（不含）；escape 为真时反斜杠是转义符。
func scanString(s string, start int, escape bool) (end int, ok bool) {
	for j := start + 1; j < len(s); j++ {
		switch s[j] {
		case '\'':
			if j+1 < len(s) && s[j+1] == '\'' {
				j++ // '' 是转义，不是结束
				continue
			}
			return j + 1, true
		case '\\':
			if escape {
				j++
			}
		}
	}
	return 0, false
}

// scanDollar 判断 start 处是否为美元引号字符串（$$...$$ 或 $tag$...$tag$），
// 是则返回其结束位置（不含）。
func scanDollar(s string, start int) (end int, ok bool) {
	j := start + 1
	for j < len(s) && (s[j] == '_' || isIdentStart(s[j]) || isDigit(s[j])) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return 0, false
	}
	delim := s[start : j+1]
	k := strings.Index(s[j+1:], delim)
	if k < 0 {
		return 0, false
	}
	return j + 1 + k + len(delim), true
}

func isWord(t tok, word string) bool {
	return t.kind == tokWord && strings.EqualFold(t.text, word)
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) || c == '$' }

func isOperator(c byte) bool { return strings.IndexByte("+-*/<>=~!@#%^&|", c) >= 0 }

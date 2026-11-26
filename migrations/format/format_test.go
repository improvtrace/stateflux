package format

import (
	"strings"
	"testing"

	"ariga.io/atlas/sql/migrate"
)

// atlasDefault 是 Atlas 缺省格式器对 task_persistents 的输出形态：一行一条语句。
const atlasDefault = `-- Create "task_persistents" table
CREATE TABLE "task_persistents" ("id" bigint NOT NULL, "resource" character varying NOT NULL, "payload" jsonb NULL, "note" character varying NOT NULL DEFAULT '', "done_at" timestamptz DEFAULT NULL NULL, "price" numeric(10,2) NULL, PRIMARY KEY ("id"));
-- Create index "taskpersistent_idempotency_key" to table: "task_persistents"
CREATE UNIQUE INDEX "taskpersistent_idempotency_key" ON "task_persistents" ("idempotency_key") WHERE ((idempotency_key)::text <> ''::text);
-- Create index "taskpersistent_resource_phase" to table: "task_persistents"
CREATE INDEX "taskpersistent_resource_phase" ON "task_persistents" ("resource", "phase");
`

const pretty = `-- Create "task_persistents" table
CREATE TABLE "task_persistents" (
    "id"       bigint NOT NULL,
    "resource" character varying NOT NULL,
    "payload"  jsonb,
    "note"     character varying NOT NULL DEFAULT '',
    "done_at"  timestamptz DEFAULT NULL,
    "price"    numeric(10,2),
    PRIMARY KEY ("id")
);

-- Create index "taskpersistent_idempotency_key" to table: "task_persistents"
CREATE UNIQUE INDEX "taskpersistent_idempotency_key"
    ON "task_persistents" ("idempotency_key")
    WHERE ((idempotency_key)::text <> ''::text);

-- Create index "taskpersistent_resource_phase" to table: "task_persistents"
CREATE INDEX "taskpersistent_resource_phase" ON "task_persistents" ("resource", "phase");
`

func TestSQL(t *testing.T) {
	if got := string(SQL([]byte(atlasDefault))); got != pretty {
		t.Fatalf("SQL:\n--- got ---\n%s\n--- want ---\n%s", got, pretty)
	}
	// 幂等：再美化一次结果不变
	if again := string(SQL([]byte(pretty))); again != pretty {
		t.Fatalf("SQL 不幂等:\n%s", again)
	}
}

// TestSQLDirective 断言文件头的指令块（如 -- atlas:txmode）及其后的空行原样保留。
func TestSQLDirective(t *testing.T) {
	const src = "-- atlas:txmode file\n\n-- comment\nDROP INDEX \"i\";\n"
	if got := string(SQL([]byte(src))); got != src {
		t.Fatalf("SQL:\n got: %q\nwant: %q", got, src)
	}
}

// TestFormatStmtWhitespaceOnly 断言美化除了省略列定义末尾冗余的 NULL 标记
// （dropNull 为省略个数）之外只调整空白：词法单元的内容与顺序和原文一致。
func TestFormatStmtWhitespaceOnly(t *testing.T) {
	cases := []struct {
		in       string
		dropNull int
	}{
		{`ALTER TABLE "t" ALTER COLUMN "c" TYPE character varying(200) USING ("c"::character varying);`, 0},
		{`CREATE TABLE "t" ("a" bigint, CONSTRAINT "fk" FOREIGN KEY ("a") REFERENCES "u" ("id") ON DELETE CASCADE, CHECK ("a" <> 0));`, 0},
		{`CREATE TABLE "t" ("a" text DEFAULT E'x\'y', "b" text NULL);`, 1},
		{`CREATE TABLE "t" ("a" text DEFAULT $$a,b(c)$$, "b" text NULL);`, 1},
		{`CREATE TABLE "t" ("x(,)" text NULL, "y,,z" text NULL);`, 2},
		{`CREATE TABLE "t" ("select" text NULL, "where" text NULL, "null" text NULL);`, 3},
		{`CREATE TABLE "t" ("a" text COLLATE "C" NULL DEFAULT 'x', "b" text NULL);`, 1},
		{`CREATE TABLE "t" ("a" text NULL, "b" text DEFAULT NULL, "c" text DEFAULT NULL NULL);`, 2},
		{`CREATE TABLE "t" ("a" bigint NOT NULL, PRIMARY KEY ("a")) WITH (fillfactor = 70);`, 0},
		{`COMMENT ON COLUMN "t"."a" IS 'it''s -- not a comment; really, (trust me)';`, 0},
		{`ALTER TABLE "t" ADD CONSTRAINT "fk" FOREIGN KEY ("a") REFERENCES "u" ("id") ON DELETE CASCADE ON UPDATE RESTRICT;`, 0},
		{`CREATE UNIQUE INDEX "i" ON "t" (lower("name"), "b" DESC) WHERE ("deleted_at" IS NULL AND "name" <> '');`, 0},
	}
	for _, c := range cases {
		got := formatStmt(c.in)
		in, ok := tokenize(c.in)
		if !ok {
			t.Fatalf("词法无法识别输入: %s", c.in)
		}
		out, ok := tokenize(got)
		if !ok {
			t.Fatalf("词法无法识别输出: %s", got)
		}
		var dropped int
		for i, j := 0, 0; i < len(in); i++ {
			if j < len(out) && in[i].text == out[j].text && in[i].kind == out[j].kind {
				j++
				continue
			}
			if dropped++; !isWord(in[i], "NULL") {
				t.Fatalf("除 NULL 外的单元被改动:\n in: %s\nout: %s", c.in, got)
			}
		}
		if dropped != c.dropNull || len(out) != len(in)-dropped {
			t.Fatalf("省略的 NULL 个数不符（got %d, want %d）:\n in: %s\nout: %s", dropped, c.dropNull, c.in, got)
		}
	}
}

// TestFormatStmtPassthrough 断言不在处理范围内的文本原样保留。
func TestSQLPassthrough(t *testing.T) {
	cases := []string{
		"CREATE TABLE \"t\" (\n  \"a\" bigint\n);",         // 多行语句
		`CREATE TABLE "t" ("a" bigint /* c */, "b" text);`, // 语句内注释
		`CREATE TABLE "t" ("a" bigint -- c
);`,
		`COMMENT ON COLUMN "t"."a" IS 'unterminated;`, // 引号不配对
		`CREATE TABLE "t" ("a" bigint;`,               // 括号不配对
		`SELECT 1`,                                    // 无分号
	}
	for _, in := range cases {
		if got := string(SQL([]byte(in + "\n"))); got != in+"\n" {
			t.Fatalf("应原样保留:\n in: %q\nout: %q", in+"\n", got)
		}
	}
}

func TestFormatter(t *testing.T) {
	plan := &migrate.Plan{
		Name: "init",
		Changes: []*migrate.Change{
			{Comment: `Create "t" table`, Cmd: `CREATE TABLE "t" ("id" bigint NOT NULL, "name" text NULL, PRIMARY KEY ("id"))`},
			{Comment: `Create "name" to table: "t"`, Cmd: `CREATE INDEX "name" ON "t" ("name")`},
		},
	}
	files, err := Formatter{}.Format(plan)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	const want = `-- Create "t" table
CREATE TABLE "t" (
    "id"   bigint NOT NULL,
    "name" text,
    PRIMARY KEY ("id")
);

-- Create "name" to table: "t"
CREATE INDEX "name" ON "t" ("name");
`
	if len(files) != 1 {
		t.Fatalf("文件个数: got %d, want 1", len(files))
	}
	if f := files[0]; !strings.HasSuffix(f.Name(), "_init.sql") || string(f.Bytes()) != want {
		t.Fatalf("生成文件不符:\n name: %s\n--- got ---\n%s\n--- want ---\n%s", f.Name(), f.Bytes(), want)
	}
	// 美化后的文件仍能被 Atlas 切回原样的语句
	stmts, err := files[0].Stmts()
	if err != nil {
		t.Fatalf("Stmts: %v", err)
	}
	if len(stmts) != 2 || !strings.HasPrefix(stmts[0], "CREATE TABLE") || stmts[1] != `CREATE INDEX "name" ON "t" ("name");` {
		t.Fatalf("Atlas 切分出的语句不符: %q", stmts)
	}
}

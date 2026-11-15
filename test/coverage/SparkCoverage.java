import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser;
import org.apache.spark.sql.catalyst.parser.UpperCaseCharStream;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.trees.TreeNode;

/**
 * Measures, with Spark 3.5's own parser and analyzer, which parts of Spark SQL a set of statements exercises.
 *
 *   SparkCoverage universe OUT JAR...          every grammar alternative (G:), keyword (TOKEN:), Catalyst expression
 *                                              class (E:) and logical plan class (P:) that exists in Spark
 *   SparkCoverage parse STATEMENTS OUT         the grammar alternatives and keywords in each statement's parse tree
 *   SparkCoverage analyze STATEMENTS OUT DIR   the classes in each statement's analyzed plan, and the error class
 *                                              (ERR:) of the statements Spark rejects; DIR is a scratch warehouse
 *
 * STATEMENTS holds one statement per record, records separated by U+0001. A record that starts with U+0002 marks
 * the start of a case file: the catalog is cleaned. OUT lists "item<TAB>number of statements containing it".
 * Driver: scripts/spark_coverage.py. Plan: docs/plans/parity-coverage-closure.md, workstream L.
 */
public class SparkCoverage {
    static void walk(ParseTree t, Map<String, Integer> seen) {
        if (t instanceof ParserRuleContext) {
            seen.merge("G:" + t.getClass().getSimpleName(), 1, Integer::sum);
        } else if (t instanceof TerminalNode) {
            int type = ((TerminalNode) t).getSymbol().getType();
            String name = SqlBaseParser.VOCABULARY.getSymbolicName(type);
            if (name != null) seen.merge("TOKEN:" + name, 1, Integer::sum);
        }
        for (int i = 0; i < t.getChildCount(); i++) walk(t.getChild(i), seen);
    }
    static void walkPlan(TreeNode<?> n, Map<String, Integer> seen) {
        seen.merge((n instanceof Expression ? "E:" : "P:") + n.getClass().getName(), 1, Integer::sum);
        if (n instanceof LogicalPlan) {
            scala.collection.Seq<Expression> ex = ((LogicalPlan) n).expressions();
            for (int i = 0; i < ex.size(); i++) walkPlan(ex.apply(i), seen);
        }
        scala.collection.Seq<?> ch = n.children();
        for (int i = 0; i < ch.size(); i++) walkPlan((TreeNode<?>) ch.apply(i), seen);
        if (n instanceof org.apache.spark.sql.catalyst.expressions.SubqueryExpression)
            walkPlan(((org.apache.spark.sql.catalyst.expressions.SubqueryExpression) n).plan(), seen);
    }

    static void universe(String[] jars, String out) throws Exception {
        try (PrintWriter w = new PrintWriter(out)) {
            Class<?>[] cs = SqlBaseParser.class.getDeclaredClasses();
            Set<Class<?>> parents = new HashSet<>();
            for (Class<?> c : cs) parents.add(c.getSuperclass());
            for (Class<?> c : cs) {
                if (!c.getSimpleName().endsWith("Context") || parents.contains(c)) continue;
                w.println("G:" + c.getSimpleName() + "\t" + c.getSuperclass().getSimpleName());
            }
            for (int i = 1; i <= SqlBaseParser.VOCABULARY.getMaxTokenType(); i++) {
                String n = SqlBaseParser.VOCABULARY.getSymbolicName(i);
                if (n != null) w.println("TOKEN:" + n);
            }
            for (String jar : jars) {
                try (java.util.jar.JarFile jf = new java.util.jar.JarFile(jar)) {
                    for (Enumeration<java.util.jar.JarEntry> e = jf.entries(); e.hasMoreElements();) {
                        String n = e.nextElement().getName();
                        if (!n.endsWith(".class") || !n.startsWith("org/apache/spark/sql/catalyst/") || n.contains("$anon")
                            || n.endsWith("$.class")) continue;
                        try {
                            Class<?> c = Class.forName(n.substring(0, n.length() - 6).replace('/', '.'), false,
                                                       SparkCoverage.class.getClassLoader());
                            if (java.lang.reflect.Modifier.isAbstract(c.getModifiers()) || c.isInterface()) continue;
                            if (Expression.class.isAssignableFrom(c)) w.println("E:" + c.getName());
                            else if (LogicalPlan.class.isAssignableFrom(c)) w.println("P:" + c.getName());
                        } catch (Throwable t) { /* a class that cannot be loaded is not part of the universe */ }
                    }
                }
            }
        }
    }
    public static void main(String[] a) throws Exception {
        if (a[0].equals("universe")) { universe(Arrays.copyOfRange(a, 2, a.length), a[1]); return; }
        String[] stmts = new String(Files.readAllBytes(Paths.get(a[1])), "UTF-8").split("\u0001");
        Map<String, Integer> seen = new TreeMap<>();
        int ok = 0, bad = 0;
        if (a[0].equals("parse")) {
            for (String s : stmts) {
                try {
                    SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(s)));
                    lexer.removeErrorListeners();
                    SqlBaseParser p = new SqlBaseParser(new CommonTokenStream(lexer));
                    p.removeErrorListeners();
                    p.setErrorHandler(new BailErrorStrategy());
                    ParseTree t = p.singleStatement();
                    Map<String, Integer> one = new HashMap<>();
                    walk(t, one);
                    for (String k : one.keySet()) seen.merge(k, 1, Integer::sum);
                    ok++;
                } catch (Exception e) { bad++; }
            }
        } else {
            SparkSession spark = SparkSession.builder().master("local[1]").config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", a[3]).getOrCreate();
            spark.sparkContext().setLogLevel("OFF");
            for (String s : stmts) {
                if (s.startsWith("\u0002RESET")) {  // new file: clean catalog
                    for (org.apache.spark.sql.catalog.Table t : (org.apache.spark.sql.catalog.Table[]) spark.catalog().listTables().collect()) {
                        try { if (t.isTemporary()) spark.catalog().dropTempView(t.name()); else spark.sql("DROP " + (t.tableType().equals("VIEW") ? "VIEW" : "TABLE") + " IF EXISTS `" + t.name() + "`"); } catch (Exception e) {}
                    }
                    continue;
                }
                try {
                    LogicalPlan plan = spark.sql(s).queryExecution().analyzed();
                    Map<String, Integer> one = new HashMap<>();
                    walkPlan(plan, one);
                    for (String k : one.keySet()) seen.merge(k, 1, Integer::sum);
                    ok++;
                } catch (Throwable e) {
                    bad++;
                    String m = String.valueOf(e.getMessage());
                    java.util.regex.Matcher mm = java.util.regex.Pattern.compile("^\\[([A-Z0-9_.]+)\\]").matcher(m);
                    if (mm.find()) seen.merge("ERR:" + mm.group(1), 1, Integer::sum);
                }
            }
            spark.stop();
        }
        try (PrintWriter w = new PrintWriter(a[2])) {
            w.println("#ok=" + ok + " bad=" + bad);
            for (Map.Entry<String, Integer> e : seen.entrySet()) w.println(e.getKey() + "\t" + e.getValue());
        }
        System.out.println("ok=" + ok + " bad=" + bad + " distinct=" + seen.size());
    }
}

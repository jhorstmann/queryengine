package net.jhorstmann.queryengine;

import net.jhorstmann.queryengine.ast.Query;
import net.jhorstmann.queryengine.data.*;
import net.jhorstmann.queryengine.evaluator.*;
import net.jhorstmann.queryengine.evaluator.Mode;
import net.jhorstmann.queryengine.operator.Operator;
import net.jhorstmann.queryengine.parser.ParserHelperKt;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@BenchmarkMode(org.openjdk.jmh.annotations.Mode.Throughput)
public class SimpleSumBenchmark {

    @State(Scope.Benchmark)
    public static class Input {
        @Param({"1000", "1000000"})
        int size;
        private Table table;
        private Operator interpretedPlan;
        private Operator closurePlan;
        private Operator bytecodePlan;
        private Schema schema;
        private List<String> projection;


        @Setup
        public void setup() {
            schema = new Schema(Arrays.asList(new Field("foo", DataType.DOUBLE), new Field("bar", DataType.DOUBLE)));
            projection = schema.getFields().stream().map(Field::getName).collect(Collectors.toList());

            List<List<Object>> values = IntStream.range(0, size).mapToObj(i -> {
                double foo = (i / 1000);
                double bar = (i / 1000);
                return List.of((Object) foo, (Object) bar);
            }).collect(Collectors.toList());

            table = new MemoryTable(schema, values);

            TableRegistry registry = new TableRegistry();
            registry.register("table", table);


            Query query = ParserHelperKt.parseQuery("SELECT SUM(foo + 10*bar) FROM table");
            LogicalNode logicalPlan = PlannerKt.buildLogicalPlan(registry, query);

            interpretedPlan = PlannerKt.buildPhysicalPlan(registry, logicalPlan, Mode.INTERPRETER);
            closurePlan = PlannerKt.buildPhysicalPlan(registry, logicalPlan, Mode.CLOSURE_COMPILER);
            bytecodePlan = PlannerKt.buildPhysicalPlan(registry, logicalPlan, Mode.BYTECODE_COMPILER);
        }
    }


    @Benchmark
    public Object runInterpreter(Input input) {
        Operator plan = input.interpretedPlan;
        plan.open();
        try {
            return plan.next();
        } finally {
            plan.close();
        }
    }

    @Benchmark
    public Object runClosureCompiler(Input input) {
        Operator plan = input.closurePlan;
        plan.open();
        try {
            return plan.next();
        } finally {
            plan.close();
        }
    }

    @Benchmark
    public Object runBytecode(Input input) {
        Operator plan = input.bytecodePlan;
        plan.open();
        try {
            return plan.next();
        } finally {
            plan.close();
        }
    }

    //@Benchmark
    public Object runNative(Input input) {
        Operator plan = input.table.getScanOperator(input.projection);
        plan.open();
        Accumulator sumAccumulator = new SumAccumulator();
        try {
            while (true) {
                Object[] row = plan.next();
                if (row == null) {
                    break;
                }
                Object x = row[0];
                if (x != null) {
                    sumAccumulator.accumulate(x);
                }
            }
        } finally {
            plan.close();
        }
        return sumAccumulator.finish();
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SimpleSumBenchmark.class.getSimpleName())
                .jvmArgs("-XX:MaxInlineLevel=16", "-XX:+UnlockExperimentalVMOptions", "-XX:+TrustFinalNonStaticFields", "-XX:+SuperWordReductions", "-XX:LoopUnrollLimit=250", "-XX:LoopMaxUnroll=16", "-XX:+PrintFlagsFinal")
                .addProfiler(LinuxPerfAsmProfiler.class)
                //.addProfiler(LinuxPerfNormProfiler.class)
                .threads(1)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}

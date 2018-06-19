package org.lsst.dax.albuquery.rewrite

import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression
import com.facebook.presto.sql.tree.DoubleLiteral
import com.facebook.presto.sql.tree.Expression
import com.facebook.presto.sql.tree.FunctionCall
import com.facebook.presto.sql.tree.Literal
import com.facebook.presto.sql.tree.LongLiteral
import com.facebook.presto.sql.tree.Node
import com.facebook.presto.sql.tree.QualifiedName
import java.math.BigDecimal
import java.util.LinkedList

class AdqlSciSqlRewriter(val isQserv: Boolean) : AstRebuilder<LinkedList<Node>>() {

    val SHAPES = setOf("circle", "box", "polygon")

    override fun process(node: Node?, context: LinkedList<Node>?): Node {
        context?.push(node)
        val ret = super.process(node, context)
        context?.pop()
        return ret
    }

    override fun visitFunctionCall(node: FunctionCall, context: LinkedList<Node>): FunctionCall {
        val arguments = processNodeItems(node.arguments, context)

        if (node.name.suffix == "contains") {
            return rewriteContainsFunction(arguments)
        }
        if (node.name.suffix == "distance") {
            return rewriteDistanceFunction(arguments)
        }
        return node
    }

    fun Literal.getBigDecimal(): BigDecimal? {
        if (this is LongLiteral) {
            return BigDecimal.valueOf(this.value)
        }
        if (this is DoubleLiteral) {
            return BigDecimal.valueOf(this.value)
        }
        return null
    }

    fun rewriteDistanceFunction(arguments: List<Expression>): FunctionCall {
        if (arguments.size == 4) {
            return FunctionCall(QualifiedName.of("scisql_angSep"), arguments)
        }
        if (arguments.size == 2) {
            val point1 = arguments[0]
            val point2 = arguments[1]
            if (point1 !is FunctionCall || point2 !is FunctionCall ||
                point1.name.suffix != "point" || point2.name.suffix != "point") {
                throw ParsingException("Wrong number of arguments to function DISTANCE (2 points expected)")
            }
            val args = arrayListOf<Expression>()
            args.addAll(point1.arguments)
            args.addAll(point2.arguments)
            return FunctionCall(QualifiedName.of("scisql_angSep"), args)
        }
        throw ParsingException("Wrong number of arguments to function DISTANCE")
    }

    fun rewriteContainsFunction(arguments: List<Expression>): FunctionCall {
        if (arguments.size != 2) {
            throw ParsingException("Wrong number of arguments to function CONTAINS (2 arguments expected)")
        }
        val pointArg = arguments[0]
        val shapeArg = arguments[1]
        if (pointArg !is FunctionCall || pointArg.name.suffix != "point") {
            throw ParsingException("Wrong first argument to function CONTAINS: Not a POINT")
        }

        if (shapeArg !is FunctionCall || !SHAPES.contains(shapeArg.name.suffix)) {
            throw ParsingException("Wrong second argument to function CONTAINS: Not a shape")
        }

        val args = arrayListOf<Expression>()

        if (!isQserv) {
            args.add(pointArg.arguments[0])
            args.add(pointArg.arguments[1])
        }

        val functionName = when (shapeArg.name.suffix) {
            "polygon" -> {
                assertConstantArgs(shapeArg.arguments)
                // If odd or less than 3 pairs
                if (shapeArg.arguments.size and 1 == 1 || shapeArg.arguments.size < 6) {
                    throw ParsingException("Wrong number of arguments for function POLYGON")
                }
                args.addAll(shapeArg.arguments)
                if (isQserv) "qserv_areaspec_poly" else "scisql_s2PtInCPoly"
            }
            "box" -> {
                if (shapeArg.arguments.size != 4) {
                    throw ParsingException("Wrong arguments for function BOX")
                }
                val constantArgs = assertConstantArgs(shapeArg.arguments)

                val ra = constantArgs[0]
                val dec = constantArgs[1]
                val width = constantArgs[2]
                val height = constantArgs[3]

                val halfWidth = width.divide(BigDecimal(2.0))
                val halfHeight = height.divide(BigDecimal(2.0))

                args.add(numericExpression(ra.subtract(halfWidth)))
                args.add(numericExpression(dec.subtract(halfHeight)))
                args.add(numericExpression(ra.add(halfWidth)))
                args.add(numericExpression(dec.add(halfHeight)))

                if (isQserv) "qserv_areaspec_box" else "scisql_s2PtInBox"
            }
            "circle" -> {
                args.addAll(shapeArg.arguments)
                if (isQserv) "qserv_areaspec_circle" else "scisql_s2PtInCircle"
            }
            else -> throw ParsingException("Unknown Spatial Function in CONTAINS")
        }
        return FunctionCall(QualifiedName.of(functionName), args)
    }

    /**
     * For databases that can't handle expressions in something that looks like a
     * CONTAINS query, we throw exceptions.
     */
    private fun assertConstantArgs(arguments: List<Expression>): List<BigDecimal> {
        val constantArgs = arrayListOf<BigDecimal>()
        for (arg in arguments) {
            val argVal = when (arg) {
                is ArithmeticUnaryExpression -> {
                    val value = arg.value
                    if (value !is Literal || value.getBigDecimal() == null) {
                        throw ParsingException("Argument is not a numeric literal")
                    }
                    // We just checked if it was null
                    val bigDecimal = value.getBigDecimal()!!
                    if (arg.sign == ArithmeticUnaryExpression.Sign.MINUS) bigDecimal.negate() else bigDecimal
                }
                is Literal -> {
                    arg.getBigDecimal() ?: throw ParsingException("Argument is not a numeric literal")
                }
                else -> throw ParsingException("Argument is not a numeric literal")
            }
            constantArgs.add(argVal)
        }
        return constantArgs
    }

    private fun numericExpression(value: BigDecimal): Expression {
        val literal = DoubleLiteral(value.abs().toString())
        if (value.signum() < 0) {
            return ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, literal)
        }
        return literal
    }
}

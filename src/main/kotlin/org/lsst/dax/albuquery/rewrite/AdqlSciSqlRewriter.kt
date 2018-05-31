package org.lsst.dax.albuquery.rewrite

import com.facebook.presto.sql.parser.ParsingException
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

    fun getParentNode(context: LinkedList<Node>): Node {
        val stackIterator = context.iterator()
        stackIterator.next() // Discard _this_ node
        return stackIterator.next()
    }

    override fun visitFunctionCall(node: FunctionCall, context: LinkedList<Node>): FunctionCall {
        val arguments = processNodeItems(node.arguments, context)

        if (node.name.suffix == "contains") {
            if (arguments.size != 2) {
                throw ParsingException("Wrong number of arguments to function CONTAINS")
            }
            val arg1 = arguments[0]
            val arg2 = arguments[1]
            if (arg1 !is FunctionCall || arg1.name.suffix != "point") {
                throw ParsingException("Wrong first argument to function CONTAINS")
            }

            if (arg2 !is FunctionCall || !SHAPES.contains(arg2.name.suffix)) {
                throw ParsingException("Wrong second argument to function CONTAINS")
            }

            val args = arrayListOf<Expression>()

            if (!isQserv) {
                args.add(arg1.arguments[0])
                args.add(arg1.arguments[1])
            }

            val functionName = when (arg2.name.suffix) {
                "polygon" -> {
                    assertConstantArgs(arg2.arguments)
                    // If odd or less than 3 pairs
                    if (arg2.arguments.size and 1 == 1 || arg2.arguments.size < 6) {
                        throw ParsingException("Wrong number of arguments for function POLYGON")
                    }
                    args.addAll(arg2.arguments)
                    if (isQserv) "qserv_areaspec_poly" else "scisql_s2PtInCPoly"
                }
                "box" -> {
                    if (arg2.arguments.size != 4) {
                        throw ParsingException("Wrong arguments for function BOX")
                    }
                    val constantArgs = assertConstantArgs(arg2.arguments)

                    val ra = constantArgs[0]
                    val dec = constantArgs[1]
                    val width = constantArgs[2]
                    val height = constantArgs[3]

                    val halfWidth = width.divide(BigDecimal(2.0))
                    val halfHeight = height.divide(BigDecimal(2.0))

                    args.add(DoubleLiteral(ra.subtract(halfWidth).toString()))
                    args.add(DoubleLiteral(ra.add(halfWidth).toString()))
                    args.add(DoubleLiteral(dec.subtract(halfHeight).toString()))
                    args.add(DoubleLiteral(dec.add(halfHeight).toString()))

                    if (isQserv) "qserv_areaspec_box" else "scisql_s2PtInBox"
                }
                "circle" -> {
                    args.addAll(arg2.arguments)
                    if (isQserv) "qserv_areaspec_circle" else "scisql_s2PtInCircle"
                }
                else -> throw ParsingException("Unknown Spatial Function in CONTAINS")
            }
            return FunctionCall(QualifiedName.of(functionName), args)
        }
        return node
    }

    fun Literal.getBigDecimal(): BigDecimal? {
        if (this is LongLiteral){
            return BigDecimal.valueOf(this.value)
        }
        if (this is DoubleLiteral) {
            return BigDecimal.valueOf(this.value)
        }
        return null
    }

    /**
     * For databases that can't handle expressions in something that looks like a
     * CONTAINS query, we throw exceptions.
     */
    private fun assertConstantArgs(arguments: List<Expression>): List<BigDecimal> {
        val constantArgs = arrayListOf<BigDecimal>()
        for (arg in arguments) {
            if (arg !is Literal) {
                throw ParsingException("Argument is not a numeric literal")
            }
            val argVal = arg.getBigDecimal() ?: throw ParsingException("Argument is not a numeric literal")
            constantArgs.add(argVal)
        }
        return constantArgs
    }
}

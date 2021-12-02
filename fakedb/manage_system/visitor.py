from ..parser import SQLVisitor, SQLParser







class SystemVisitor(SQLVisitor):
    '''
    派生visitor
    利用SystemManager封装的接口, 在访问语法树的过程中完成sql语句的执行
    '''
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        
    # Visit a parse tree produced by SQLParser#program.
    def visitProgram(self, ctx:SQLParser.ProgramContext):
        '''
        根节点
        顺序执行多条query
        '''
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#create_db.
    def visitCreate_db(self, ctx:SQLParser.Create_dbContext):
        # TODO: manager实现函数
        return self.manager.create_db(ctx.Identifier().getText())

    # Visit a parse tree produced by SQLParser#drop_db.
    def visitDrop_db(self, ctx:SQLParser.Drop_dbContext):
        # TODO: manager实现函数
        return self.manager.drop_db(ctx.Identifier().getText())

    # Visit a parse tree produced by SQLParser#show_dbs.
    def visitShow_dbs(self, ctx:SQLParser.Show_dbsContext):
        # TODO: manager实现函数
        return self.manager.show_dbs()

    # Visit a parse tree produced by SQLParser#use_db.
    def visitUse_db(self, ctx:SQLParser.Use_dbContext):
        # TODO: manager实现函数
        return self.manager.use_db(ctx.Identifier().getText())

    # Visit a parse tree produced by SQLParser#show_tables.
    def visitShow_tables(self, ctx:SQLParser.Show_tablesContext):
        # TODO:
        return self.manager.show_tables()

    # Visit a parse tree produced by SQLParser#show_indexes.
    def visitShow_indexes(self, ctx:SQLParser.Show_indexesContext):
        # TODO:
        return self.manager.show_indexes()

    # Visit a parse tree produced by SQLParser#load_data.
    def visitLoad_data(self, ctx:SQLParser.Load_dataContext):
        '''
        io statement
        '''
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#dump_data.
    def visitDump_data(self, ctx:SQLParser.Dump_dataContext):
        '''
        io statement
        '''
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#create_table.
    def visitCreate_table(self, ctx:SQLParser.Create_tableContext):
        # TODO:
        columns, foreign_keys, primary = ctx.field_list().accept(self)
        table = ctx.Identifier().getText()
        res = self.manager.create_table()
        for key in foreign_keys:
            self.manager.add_foreign(table, key, foreign_keys[key])
        self.manager.set_primary(table, primary)
        return res

    # Visit a parse tree produced by SQLParser#drop_table.
    def visitDrop_table(self, ctx:SQLParser.Drop_tableContext):
        # TODO:
        table = ctx.Identifier().getText()
        return self.manager.drop_table(table)

    # Visit a parse tree produced by SQLParser#describe_table.
    def visitDescribe_table(self, ctx:SQLParser.Describe_tableContext):
        table = ctx.Identifier().getText()
        return self.manager.describe_table(table)

    # Visit a parse tree produced by SQLParser#insert_into_table.
    def visitInsert_into_table(self, ctx:SQLParser.Insert_into_tableContext):
        # TODO:
        table = ctx.getChild(2)
        value_lists = ctx.value_lists().accept(self)
        for i in value_lists:
            self.manager.insert(table, i)
        return None

    # Visit a parse tree produced by SQLParser#delete_from_table.
    def visitDelete_from_table(self, ctx:SQLParser.Delete_from_tableContext):
        # TODO:
        table = ctx.Identifier().getText()
        conditions = ctx.where_and_clause().accept(self)
        return self.manager.delete_records(table, conditions)

    # Visit a parse tree produced by SQLParser#update_table.
    def visitUpdate_table(self, ctx:SQLParser.Update_tableContext):
        # TODO:
        table = ctx.Identifier().getText()
        conditions = ctx.where_and_clause().accept(self)
        update_info = ctx.set_clause().accept(self)
        return self.manager.update(table, conditions, update_info)

    # Visit a parse tree produced by SQLParser#select_table.
    def visitSelect_table(self, ctx:SQLParser.Select_tableContext):
        # TODO:
        table = ctx.identifiers().accept(self)
        conditions = ctx.where_and_clause().accept(self) if ctx.where_and_clause() else ()
        selectors = ctx.selectors.accept(self)
        group_by = ctx.column().accept(self) if ctx.column() else (None, '')
        limit = int(ctx.Integer(0).getText()) if ctx.Integer(0) else None
        offset = int(ctx.Integer(1).getText()) if ctx.Integer(1) else None
        return self.manager.select(selectors, table, conditions, group_by, limit, offset)

    # Visit a parse tree produced by SQLParser#alter_add_index.
    def visitAlter_add_index(self, ctx:SQLParser.Alter_add_indexContext):
        # TODO:
        table= ctx.Identifier(0).getText()
        index = ctx.Identifier(1).getText()
        cols = ctx.identifiers().accept(self)
        for col in cols:
            self.manager.create_index(index, table, col)

    # Visit a parse tree produced by SQLParser#alter_drop_index.
    def visitAlter_drop_index(self, ctx:SQLParser.Alter_drop_indexContext):
        # TODO:
        index = ctx.Identifier(1).getText()
        return self.manager.drop_index(index)

    # Visit a parse tree produced by SQLParser#alter_table_drop_pk.
    def visitAlter_table_drop_pk(self, ctx:SQLParser.Alter_table_drop_pkContext):
        # TODO
        table = ctx.Identifier(0).getText()
        return self.manager.drop_primary_key(table)

    # Visit a parse tree produced by SQLParser#alter_table_drop_foreign_key.
    def visitAlter_table_drop_foreign_key(self, ctx:SQLParser.Alter_table_drop_foreign_keyContext):
        # TODO:
        foreign_key = ctx.Identifer(1).getText()
        return self.manager.drop_foreign_key(foreign_key)

    # Visit a parse tree produced by SQLParser#alter_table_add_pk.
    def visitAlter_table_add_pk(self, ctx:SQLParser.Alter_table_add_pkContext):
        # TODO:
        table = ctx.Identifier(0).getText()
        primary_key = ctx.identifiers().accept(self)
        return self.manager.set_primary_key(table, primary_key)

    # Visit a parse tree produced by SQLParser#alter_table_add_foreign_key.
    def visitAlter_table_add_foreign_key(self, ctx:SQLParser.Alter_table_add_foreign_keyContext):
        # TODO:
        table = ctx.Identifier(0).getText()
        primary_key = ctx.identifiers().accept(self)
        return self.manager.set_primary(table, primary_key)

    # Visit a parse tree produced by SQLParser#alter_table_add_unique.
    def visitAlter_table_add_unique(self, ctx:SQLParser.Alter_table_add_uniqueContext):
        # TODO:
        table = None
        name = None
        col = None
        return self.manager.add_unique(table, col, name)

    # Visit a parse tree produced by SQLParser#field_list.
    def visitField_list(self, ctx:SQLParser.Field_listContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#normal_field.
    def visitNormal_field(self, ctx:SQLParser.Normal_fieldContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#primary_key_field.
    def visitPrimary_key_field(self, ctx:SQLParser.Primary_key_fieldContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#foreign_key_field.
    def visitForeign_key_field(self, ctx:SQLParser.Foreign_key_fieldContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#type_.
    def visitType_(self, ctx:SQLParser.Type_Context):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#value_lists.
    def visitValue_lists(self, ctx:SQLParser.Value_listsContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#value_list.
    def visitValue_list(self, ctx:SQLParser.Value_listContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#value.
    def visitValue(self, ctx:SQLParser.ValueContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_and_clause.
    def visitWhere_and_clause(self, ctx:SQLParser.Where_and_clauseContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_operator_expression.
    def visitWhere_operator_expression(self, ctx:SQLParser.Where_operator_expressionContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_operator_select.
    def visitWhere_operator_select(self, ctx:SQLParser.Where_operator_selectContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_null.
    def visitWhere_null(self, ctx:SQLParser.Where_nullContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_in_list.
    def visitWhere_in_list(self, ctx:SQLParser.Where_in_listContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_in_select.
    def visitWhere_in_select(self, ctx:SQLParser.Where_in_selectContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#where_like_string.
    def visitWhere_like_string(self, ctx:SQLParser.Where_like_stringContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#column.
    def visitColumn(self, ctx:SQLParser.ColumnContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#expression.
    def visitExpression(self, ctx:SQLParser.ExpressionContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#set_clause.
    def visitSet_clause(self, ctx:SQLParser.Set_clauseContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#selectors.
    def visitSelectors(self, ctx:SQLParser.SelectorsContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#selector.
    def visitSelector(self, ctx:SQLParser.SelectorContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#identifiers.
    def visitIdentifiers(self, ctx:SQLParser.IdentifiersContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#operator.
    def visitOperator(self, ctx:SQLParser.OperatorContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by SQLParser#aggregator.
    def visitAggregator(self, ctx:SQLParser.AggregatorContext):
        return self.visitChildren(ctx)

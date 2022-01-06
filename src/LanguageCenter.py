class LanguageCenter:
    #{"$and": {"user_id": {"$and": {{"$gte": 3}, 1}}, "sw": {"user_name": "Al"}}}
    def __init__(self):
        self.operator_tokens = {"$and", "$or"}
        self.comparison_tokens= {"$gt", "$lt", "$sw"}
        self.fields = set()

    def add_field(self, field):
        """
        Adds a field to the list of available fields.
        :param field: Field to add to the database.
        :return: None
        """
        self.fields.add(field)

    # TODO language center process
    def process(self, query):
        """
        Processes a provided query.
        :param query: The query to be executed.
        :return: Set of primary keys satisfying all query requirements.
        """
        return_set = set()
        for key in list(query.keys()):
            if key in self.operator_tokens:
                return_set = self.process_operator(key, query[key])
            elif key in self.comparison_tokens:
                return_set = self.process_comparison(key, query[key])
            else:
                return_set = self.process_field_query(key, query[key])
        return return_set

    # TODO language center process operator
    def process_operator(self, key, query):
        return_set = set()
        if (key == "$and"):
            pass
            # handle and
        if (key == "$or"):
            pass
            # handle or

        return return_set

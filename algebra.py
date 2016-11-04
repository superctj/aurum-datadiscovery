import itertools
import re

from modelstore.elasticstore import KWType

from api.apiutils import compute_field_id as id_from
from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Scope
from api.apiutils import Relation
from api.apiutils import DRS
from api.apiutils import DRSMode
from api.apiutils import Hit
from api.apiutils import MDClass
from api.apiutils import MDRelation


class Algebra:

    def __init__(self, network, store_client):
        self._network = network
        self._store_client = store_client

    """
    Metadata API
    """
    def annotate(self, author: str, description: str, md_class: MDClass,
        general_input, ref={"general_target": None, "type": None}):
        """
        Create a new annotation in the elasticsearch graph as metadata. Parses tags
        as keywords that follow a # symbol i.e. #data.
        :param author: identifiable name of user or process
        :param description: free text description
        :param md_class: MDClass
        :param nid, node tuple, Hit, or DRS: source(s)
        :param ref: (optional) {
            "general_target": nid, node tuple, Hit, or DRS of target(s),
            "type": MDRelation
        }
        """
        drs_source = self._general_to_drs(general_input)
        drs_target = self._general_to_drs(ref["general_target"])
        
        if drs_source.mode != DRSMode.FIELDS or drs_target.mode != DRSMode.FIELDS:
            raise ValueError("source and targets must be columns")

        tags = re.findall("(?<=#)[a-zA-Z0-9]+", description)
        tags = [tag.lower() for tag in tags]
        md_class = self._mdclass_to_str(md_class)

        # non-relational metadata
        if ref["type"] is None:
            for hit_source in drs_source:
                res = self._store_client.write_metadata(
                    author=author,
                    description=description,
                    md_class=md_class,
                    source=hit_source.nid,
                    tags=tags)
                if not res["created"]:
                    return False
            return True

        # relational metadata
        md_relation = self._mdrelation_to_str(ref["type"])
        for hit_source in drs_source:
            for hit_target in drs_target:
                res = self._store_client.write_metadata(
                    author=author,
                    description=description,
                    md_class=md_class,
                    source=hit_source.nid,
                    ref={"target": hit_target.nid, "type": md_relation},
                    tags=tags)
                if not res["created"]:
                    return False
            return True

    def add_comments(self, author: str, md_id: str, comments: list):
        """
        Add comment to metadata with the given md_id.
        :param md_id: metadata id
        :param comments: list of comments
        """
        return self._store_client.extend_field(author, "comments", md_id, comments)

    def add_tags(self, author: str, md_id: str, tags: list):
        """
        Add tags/keywords to metadata with the given md_id.
        :param md_id: metadata id
        :param tags: a list of tags to add
        """
        return self._store_client.extend_field(author, "tags", md_id, tags)

    def metadata_search(self, nid):
        """
        Given an nid, searches for all metadata that mention it
        :param nid: nid to search for
        """
        return self._store_client.get_metadata_about(str(nid))

    def pretty_print_md(self, metadata):
        """
        Pretty prints metadata documents.
        :param metadata: list of metadata documents returned by elasticsearch
        """
        for md in metadata["hits"]["hits"]:
            md_id = md["_id"]
            source = md["_source"]["source"]
            description = md["_source"]["description"]
            ref_target = md["_source"]["ref_target"]
            ref_type = md["_source"]["ref_type"]

            if ref_target is None:
                relation = "{}".format(source)
            else:
                relation = "{} {} {}".format(source, ref_type, ref_target)

            print("ID: {0:20} RELATION: {1:40} DESCRIPTION: {2}".format(md_id, relation, description))

    def pretty_print_nid(self, nid: str):
        """
        Pretty prints sourceName and columnName of given nid.
        """
        sourceName, columnName = self._store_client.get_readable_doc_with_nid(nid)
        print("({0}, {1}, {2})".format(nid, sourceName, columnName))

    """
    Basic API
    """

    def keyword_search(self, kw: str, scope: Scope, max_results=10) -> DRS:
        """
        Performs a keyword search over the contents of the data.
        Scope specifies where elasticsearch should be looking for matches.
        i.e. table titles (SOURCE), columns (FIELD), or comment (SOURCE)

        :param kw: the keyword to serch
        :param max_results: maximum number of results to return
        :return: returns a DRS
        """

        kw_type = self._scope_to_kw_type(scope)
        hits = self._store_client.search_keywords(
            keywords=kw, elasticfieldname=kw_type, max_hits=max_results)

        # materialize generator
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))
        return drs

    def neighbor_search(self,
                        general_input,
                        relation: Relation,
                        max_hops=None):
        """
        Given an nid, node, hit or DRS, finds neighbors with specified
        relation.
        :param nid, node tuple, Hit, or DRS:
        """
        # convert whatever input to a DRS
        i_drs = self._general_to_drs(general_input)

        # prepare an output DRS
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)

        # get all of the table Hits in a DRS, if necessary.
        if i_drs.mode == DRSMode.TABLE:
            self._general_to_field_drs(i_drs)

        # Check neighbors
        for h in i_drs:
            hits_drs = self._network.neighbors_id(h, relation)
            o_drs = o_drs.absorb(hits_drs)
        return o_drs

    """
    TC API
    """

    def paths(self, primitives, a: DRS, b=None, max_hops=2) -> DRS:
        """
        Is there a transitive relationship between any element in a with any
        element in b?
        This function finds the answer constrained on the primitive
        (singular for now) that is passed as a parameter.
        If b is not passed, assumes the user is searching for paths between
        elements in a.
        :param a:
        :param b:
        :param primitives:
        :return:
        """
        # create b if it wasn't passed in.
        a = self._general_to_drs(a)
        b = b or a

        self._assert_same_mode(a, b)

        # absorb the provenance of both a and b
        o_drs = DRS([], Operation(OP.NONE))
        o_drs.absorb_provenance(a)
        if b != a:
            o_drs.absorb_provenance(b)

        for h1, h2 in itertools.product(a, b):

            # test to see if a and b are different DRS's that share
            # the same element
            # I'm not sure if this is really a feature or a bug,
            # but am carrying it over from ddapi
            if a != b and h1 == h2:
                return o_drs

            # there are different network operations for table and field mode
            res_drs = None
            if a.mode == DRSMode.FIELDS:
                res_drs = self._network.find_path_hit(
                    h1, h2, primitives, max_hops=max_hops)
            else:
                res_drs = self._network.find_path_table(
                    h1, h2, primitives, self, max_hops=max_hops)

            o_drs = o_drs.absorb(res_drs)

        return o_drs

    def traverse(self, a: DRS, primitive, max_hops=2) -> DRS:
        """
        Conduct a breadth first search of nodes matching a primitive, starting
        with an initial DRS.
        :param a: a nid, node, tuple, or DRS
        :param primitive: The element to search
        :max_hops: maximum number of rounds on the graph
        """
        a = self._general_to_drs(a)

        o_drs = DRS([], Operation(OP.NONE))

        if a.mode == DRSMode.TABLE:
            raise ValueError(
                'input mode DRSMode.TABLE not supported')

        fringe = a
        o_drs.absorb_provenance(a)
        while max_hops > 0:
            max_hops = max_hops - 1
            for h in fringe:
                hits_drs = self.__network.neighbors_id(h, primitive)
                o_drs = self.union(o_drs, hits_drs)
            fringe = o_drs  # grow the initial input
        return o_drs

    """
    Combiner API
    """

    def intersection(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are both in a and b
        :param a: an iterable object
        :param b: another iterable object
        :return: the intersection of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.intersection(b)
        return o_drs

    def union(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.union(b)
        return o_drs

    def difference(self, a: DRS, b: DRS) -> DRS:
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.set_difference(b)
        return o_drs

    """
    Helper Functions
    """

    def _scope_to_kw_type(self, scope: Scope) -> KWType:
        """
        Converts a relation scope to a keyword type for elasticsearch.
        """
        kw_type = None
        if scope == Scope.DB:
            raise ValueError('DB Scope is not implemeneted')
        elif scope == Scope.SOURCE:
            kw_type = KWType.KW_TABLE
        elif scope == Scope.FIELD:
            kw_type = KWType.KW_SCHEMA
        elif scope == Scope.CONTENT:
            kw_type = KWType.KW_TEXT

        return kw_type

    def _general_to_drs(self, general_input) -> DRS:
        """
        Given an nid, node, hit, or DRS and convert it to a DRS.
        :param nid: int
        :param node: (db_name, source_name, field_name)
        :param hit: Hit
        :param DRS: DRS
        :return: DRS
        """
        # test for DRS initially for speed
        if isinstance(general_input, DRS):
            return general_input

        if general_input is None:
            general_input = DRS(data=[], operation=Operation(OP.NONE))
        if isinstance(general_input, int) or isinstance(general_input, str):
            general_input = self._nid_to_hit(general_input)
        # Hit is a subclassed from tuple
        if (isinstance(general_input, tuple) and
                not isinstance(general_input, Hit)):
            general_input = self._node_to_hit(general_input)
        if isinstance(general_input, Hit):
            general_input = self._hit_to_drs(general_input)
        if isinstance(general_input, DRS):
            return general_input

        raise ValueError(
            'Input is not None, an integer, field tuple, Hit, or DRS')

    def _nid_to_hit(self, nid: int) -> Hit:
        """
        Given a node id, convert it to a Hit
        :param nid: int or string
        :return: DRS
        """
        nid = str(nid)
        score = 0.0
        nid, db, source, field = self._network.get_info_for([nid])[0]
        hit = Hit(nid, db, source, field, score)
        return hit

    def _node_to_hit(self, node: (str, str, str)) -> Hit:
        """
        Given a field and source name, it returns a Hit with its representation
        :param node: a tuple with the name of the field,
            (db_name, source_name, field_name)
        :return: Hit
        """
        db, source, field = node
        nid = id_from(db, source, field)
        hit = Hit(nid, db, source, field, 0)
        return hit

    def _hit_to_drs(self, hit: Hit, table_mode=False) -> DRS:
        """
        Given a Hit, return a DRS. If in table mode, the resulting DRS will
        contain Hits representing that table.
        :param hit: Hit
        :param table_mode: if the Hit represents an entire table
        :return: DRS
        """
        drs = None
        if table_mode:
            table = hit.source_name
            hits = self._network.get_hits_from_table(table)
            drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]))
        else:
            drs = DRS([hit], Operation(OP.ORIGIN))

        return drs

    def _general_to_field_drs(self, general_input):
        drs = self._general_to_drs(general_input)

        drs.set_fields_mode()
        for h in drs:
            fields_table = self._hit_to_drs(h, table_mode=True)
            drs = drs.absorb(fields_table)

        return drs

    def _mdclass_to_str(self, md_class: MDClass):
        ref_table = {
            MDClass.WARNING: "warning",
            MDClass.INSIGHT: "insight",
            MDClass.QUESTION: "question"
        }
        return ref_table[md_class]

    def _mdrelation_to_str(self, md_relation: MDRelation):
        ref_table = {
            MDRelation.MEANS_SAME_AS: "MEANS SAME AS",
            MDRelation.MEANS_DIFF_FROM: "MEANS DIFF FROM",
            MDRelation.IS_SUBCLASS_OF: "IS SUBCLASS OF",
            MDRelation.IS_SUPERCLASS_OF: "IS SUPERCLASS OF",
            MDRelation.IS_MEMBER_OF: "IS MEMBER OF",
            MDRelation.IS_CONTAINER_OF: "CONTAINS"
        }
        return ref_table[md_relation]

    def _assert_same_mode(self, a: DRS, b: DRS) -> None:
        error_text = ("Input parameters are not in the same mode ",
                      "(fields, table)")
        assert a.mode == b.mode, error_text


class API(Algebra):
    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)


if __name__ == '__main__':
    print("Aurum API")

import json
import re

from qbrain.utils.file.flatten_dict import flatten_attributes

from functools import lru_cache


class GraphAttrOptimizer:
    def __init__(self):
        self.db_map = {
            # Add more known mappings if needed
            "ens": "Ensembl",
            "refseq": "RefSeq",
            "refseq_peptide": "RefSeq (Peptide)",
            "refseq_mrna": "RefSeq (mRNA)",
            "uniprot": "UniProt",
            "uniprot_gn": "UniProt Gene",
            "ncbi": "NCBI",
            "genecards": "GeneCards",
            "chebi": "ChEBI",
            "gtex": "GTEx",
            "geo": "GEO",
            "sra": "SRA",
            "ebi": "EBI",
            "dbsnp": "dbSNP",
            "pubchem": "PubChem",
            "arrayexpress": "ArrayExpress",
            "hgnc": "HGNC",
            "go": "GO",
            "omim": "OMIM",
            "mim_gene": "OMIM Gene",
            "mim_morbid": "OMIM Morbid",
            "pdb": "PDB",
            "kegg": "KEGG",
            "pfam": "Pfam",
            "panther": "PANTHER",
            "interpro": "InterPro",
            "prosite_patterns": "Prosite (Patterns)",
            "prosite_profiles": "Prosite (Profiles)",  # ✅ Added
            "embl": "EMBL",
            "ccds": "CCDS",
            "ucsc": "UCSC",
            "wikigene": "WikiGene",
            "biogrid": "BioGRID",  # ✅ Normalized to lowercase key
            "cdd": "CDD",  # ✅ Added
            "uniparc": "UniParc",  # ✅ Added
            "superfamily": "SuperFamily",  # ✅ Added
            "smart": "Smart",  # ✅ Added
            "ncbifam": "NCBIfam",  # ✅ Added
            "gene3d": "Gene3D",  # ✅ Added
            "pirsf": "PIRSF",  # ✅ Added
            "mobidblite": "MobiDBLite",  # ✅ Added
            "ncoils": "ncoils",  # ✅ Added
            "Reactome": "Reactome",  # ✅ Added

            # Relationship-based mappings
            "alphafold": "has_predicted_structure",
            "protein_id": "has_protein_id",
            "hpa": "linked_to_protein_expression",
            "seg": "has_segment_annotation",
            "entrezgene": "mapped_to_entrez_id",
            "coord_system": "aligned_to_coordinate_system",
            "strand": "located_on_strand",
            "transcripts": "contains_transcripts",
            "exons": "contains_exons"
        }

    def clean_attr_keys(self, attrs):
        cleaned_attrs = {}
        attrs = flatten_attributes(attrs)

        seen = set()
        for k, v in attrs.items():
            clean_key = self.replace_special_chars(k)
            if clean_key in seen:
                continue
            seen.add(clean_key)

            v = self.stringify_dict(v)
            cleaned_attrs[clean_key] = v

            if isinstance(v, (int, float)):
                cleaned_attrs[clean_key] = str(v)

        for k, v in cleaned_attrs.items():
            if isinstance(v, str):
                cleaned_attrs[k] = v.replace("'", "")

        return self.manipulate(cleaned_attrs)

    def manipulate(self, attrs):
        nt = attrs.get("type")
        src_layer = attrs.get("src_layer")
        trgt_layer = attrs.get("trgt_layer")

        if src_layer:
            attrs["src_layer"] = self.layer_from_key(src_layer)
        if trgt_layer:
            attrs["trgt_layer"] = self.layer_from_key(trgt_layer)
        if nt:
            attrs["type"] = self.layer_from_key(nt)

        nt = attrs["type"]
        if nt:
            if nt.upper() == "RHSA":
                self.refine_reactome(attrs)
            elif nt.upper() in ["TRANSCRIPT", "GENE", "TRANSLATION"]:
                self.refine_gene_or_ancestors(attrs)
        return attrs

    def refine_gene_or_ancestors(self, attrs):
        for key in ["exons", "xrefs", "GO"]:
            attrs.pop(key, None)

        remove_keys = [
            k for k, v in attrs.items()
            if isinstance(v, list) and all(isinstance(i, (str, int)) for i in v) and k != "parent"
        ]
        for key in remove_keys:
            attrs.pop(key, None)

    def refine_reactome(self, attrs):
        rid = attrs.get("id")
        rrid = f"Reactome:{rid}"
        if "info" in attrs and rrid in attrs["info"]:
            attrs["info"] = attrs["info"].replace(rrid, "")

    def stringify_dict(self, v):
        if isinstance(v, dict):
            v = json.dumps(v)
        elif isinstance(v, list):
            new_v = []
            for value in v:
                if isinstance(value, dict):
                    new_v.append(json.dumps(value))
                else:
                    new_v.append(value)
            v = new_v
        return v

    @lru_cache(maxsize=512)
    def replace_special_chars(self, key):
        return re.sub(r'[^a-zA-Z0-9_]', '', key)

    @lru_cache(maxsize=512)
    def layer_from_key(self, key):
        # print("key", key)
        if key is not None:
            k = key.lower()
            if "reactome" in k:
                return "RHSA"
            elif "uniprot" in k:
                return "PROTEIN"
            elif k.startswith("ensg"):
                return "GENE"
            elif k.startswith("ense"):
                return "EXON"
            elif k.startswith("enst"):
                return "TRANSCRIPT"
            elif k.startswith("ensp"):
                return "PROTEIN"
            elif k.startswith("ensr"):
                return "REGULATORY_FEATURE"
            elif "entrezgene" in k and "trans" in k and "name" in k:
                return "ENTREZGENE"
            for dkey in self.db_map:
                if dkey in k:
                    return self.db_map[dkey].upper()
            return key.upper().replace(" ", "_")


class Manipulator:

    def __init__(self):
        print("Manipulator initialized")



    def replace_special_chars(self, s):
        """
        Replaces all special characters in a string with "_".
        Keeps only alphanumeric characters and underscores.

        :param s: Input string
        :return: Cleaned string with special characters replaced
        """
        return re.sub(r'[^a-zA-Z0-9_]', '', s)

    def manipulator_dictribnutor(self, attrs, gene=False):
        nt = attrs.get("type")
        src_layer = attrs.get("src_layer", None)
        trgt_layer = attrs.get("trgt_layer", None)
        # print("nt, src_layer, trgt_layer", nt, src_layer, trgt_layer)

        if src_layer:
            attrs["src_layer"] = src_layer.upper().replace(" ", "_")
        if trgt_layer:
            attrs["trgt_layer"] = trgt_layer.upper().replace(" ", "_")
        if nt:
            attrs["type"] = nt.upper().replace(" ", "_")

        return attrs

    def clean_attr_keys(self, attrs, flatten=True, stringify=False):
        """
        Cleans attribute dictionary by:
        - Flattening nested attributes.
        - Removing duplicate keys after replacing special characters.
        - Ensuring consistency in column names.
        """

        cleaned_attrs = {}
        if flatten:
            attrs = flatten_attributes(attrs)

        for k, v in attrs.items():
            clean_key = self.replace_special_chars(k)
            if clean_key in cleaned_attrs:
                continue
            else:
                if stringify is True:
                    v = self.stringify_dict(v)
                cleaned_attrs[clean_key] = v

        for k, v in cleaned_attrs.items():
            if isinstance(v, str):
                cleaned_attrs[k] = v.replace("'", "")

        cleaned_attrs = self.manipulator_dictribnutor(cleaned_attrs)
        return cleaned_attrs

    def stringify_dict(self, v):
        if isinstance(v, dict):
            v = json.dumps(v)
        elif isinstance(v, list):
            new_v = []
            for value in v:
                if isinstance(value, dict):
                    new_v.append(json.dumps(value))
                else:
                    new_v.append(value)
            v = new_v
        return v

    def refine_gene_or_anchestors(self, attrs):
        """
        Filter xrefs
        """
        for key in ["exons", "xrefs", "GO"]:
            attrs.pop(key, None)
        xref_list = [
            k for k, v in attrs.items()
            if isinstance(v, list)
               and all(isinstance(value, (str, int)) for value in v)
               and k != "parent"
        ]

        for item in xref_list:
            attrs.pop(item)

    def refine_reactome(self, item):
        rid = item.get('id')
        rrid = f"Reactome:{rid}"
        if "info" in item and rrid in item["info"]:
            item["info"] = item["info"].replace(rrid, "")
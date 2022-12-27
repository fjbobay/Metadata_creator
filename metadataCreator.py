import difflib
import time
import itertools
import numpy as np
import math
import json
import pandas as pd
import pyarrow as pa
import os

class Metadata:
    def __init__(self):
        """Inicializar atributos de la clase"""
        self.filename = None
        self.datasets = None
        self.datanames = None
        self.metadatos = None
        self.inventary = None
        self.data_relation = None
        self.result_data_relation = None
        self.col_data_invent = None
        self.result_col_data_invent = None

    def ReadXLSX(self, file_input):
        """Cargar archivo de entrada en un diccionario de dataframes de pandas"""
        self.filename = file_input
        xlsx = pd.read_excel(file_input, sheet_name=None)
        self.datasets = list(xlsx.values())
        self.datanames = list(xlsx.keys())

    def CreateMetadata(self, version="1.0.0", title=""):
        """Obtener metadatos del archivo"""
        current_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        self.metadatos = {
            'user': os.environ['USERNAME'],
            'version': version,
            'type': title,
            'create': current_time,
            'filename': self.filename,
            'datasets': []
        }

        for dataset, dataname in zip(self.datasets, self.datanames):
            self.table = pa.Table.from_pandas(dataset)
            self.tabla = self.table.schema.metadata
            for k, n in self.tabla.items():
                inventary = json.loads(n.decode())
                inventary["name"] = dataname
                self.metadatos['datasets'].append(inventary)

    def SaveMetadata(self, filename="metadatos"):
        """Serializar json y guardar metadatos en un archivo"""
        with open(filename+".json", "w") as outfile:
            json.dump(self.metadatos, outfile, indent=2)

    def GetDatasets(self):
        """Obtener inventario de columnas de los dataframes del archivo de entrada"""
        inventary = []
        for dataset_a, dataname_a in itertools.product(self.datasets, self.datanames):
            for dataset_b, dataname_b in itertools.product(self.datasets, self.datanames):
                if not dataname_a == dataname_b:
                    cols_a = dataset_a.columns.values
                    cols_b = dataset_b.columns.values
                    intersection = np.intersect1d(cols_a, cols_b)
                    self.inventary.extend(intersection)

    def GetRalationsDatasets(self):
        """Obtener relaciones de los datos de las columnas de los dataframes del archivo de entrada"""
        self.data_relation = []
        for col in self.inventary:
            for dataset_a, dataname_a in itertools.product(self.datasets, self.datanames):
                for dataset_b, dataname_b in itertools.product(self.datasets, self.datanames):
                    if not dataname_a == dataname_b:
                        s1 = dataset_a[col].drop_duplicates().values
                        s2 = dataset_b[col].drop_duplicates().values
                        len_s1 = len(s1)
                        len_s2 = len(s2)
                        if not math.isnan(s1).any() and not math.isnan(s2).any():
                            sm1 = round(difflib.SequenceMatcher(None, s1, s2).ratio(), 2)
                            sm2 = round(difflib.SequenceMatcher(None, s2, s1).ratio(), 2)
                            relacion = str(round(((sm1 + sm2) / 2) * 100, 2)) + "%"
                        else:
                            relacion = "Fail"
                            len_s1 = 0
                            len_s2 = 0
                        self.data_relation.append({
                            "column": col,
                            "dataColA": dataname_a,
                            "lenColA": len_s1,
                            "dataColB": dataname_b,
                            "lenColB": len_s2,
                            "distance": relacion
                        })

        self.result_data_relation = pd.DataFrame(self.data_relation)

    def ExportRalitions(self, filename):
        """Guardar resultados en archivo de salida"""
        with pd.ExcelWriter(filename +".xlsx") as writer:
            self.result_col_data_invent.to_excel(writer, sheet_name="Col Data", index=False)
            self.result_data_relation.to_excel(writer, sheet_name="Col Relation", index=False)
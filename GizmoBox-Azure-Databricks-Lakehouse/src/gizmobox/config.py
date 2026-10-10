"""Configuration helpers shared by Databricks pipeline stages."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    ops_schema: str = "ops"
    landing_root: str = ""
    checkpoint_root: str = ""

    def table(self, layer: str, name: str) -> str:
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "ops": self.ops_schema,
        }[layer]
        return f"`{self.catalog}`.`{schema}`.`{name}`"

    def source_path(self, *parts: str) -> str:
        root = self.landing_root.rstrip("/")
        suffix = "/".join(part.strip("/") for part in parts)
        return f"{root}/{suffix}" if suffix else root

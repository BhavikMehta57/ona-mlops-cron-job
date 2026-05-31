from __future__ import annotations

from collections import OrderedDict

from cron_job.schemas.models import CanonicalTrait, ProtocolTraitGroup, ScannedDocument


TRAIT_KEYWORDS: tuple[tuple[str, CanonicalTrait], ...] = (
    ("pod", "pods"),
    ("flower", "flowering"),
    ("stand", "plantstand"),
)

INFERENCE_TRAIT_TYPES: dict[CanonicalTrait, str] = {
    "pods": "pod",
    "flowering": "flower",
    "plantstand": "plant_stand",
}


def canonical_trait_from_value(value: object) -> CanonicalTrait | None:
    text = str(value or "").strip().lower()
    for keyword, canonical in TRAIT_KEYWORDS:
        if keyword in text:
            return canonical
    return None


def inference_trait_type(canonical_trait: CanonicalTrait) -> str:
    return INFERENCE_TRAIT_TYPES[canonical_trait]


def group_documents_by_protocol_trait(
    documents: list[ScannedDocument],
    logger,
    *,
    trial_id: str | None = None,
    subtrial_id: str | None = None,
    image_prefixes_by_document_id: dict[str, str] | None = None,
) -> list[ProtocolTraitGroup]:
    grouped: "OrderedDict[tuple[str, CanonicalTrait], dict[str, object]]" = OrderedDict()
    image_prefixes_by_document_id = image_prefixes_by_document_id or {}

    for doc in documents:
        canonical = canonical_trait_from_value(doc.trait)
        if canonical is None:
            logger.log(
                "protocol_trait_resolution",
                "warning",
                trial_id=trial_id,
                subtrial_id=subtrial_id,
                trait_value=doc.trait,
                document_id=doc.document_id,
                errors="unrecognized trait value, skipping group",
            )
            continue

        protocol = str(doc.protocol or "").strip()
        if not protocol:
            logger.log(
                "protocol_trait_resolution",
                "warning",
                trial_id=trial_id,
                subtrial_id=subtrial_id,
                trait_value=doc.trait,
                document_id=doc.document_id,
                errors="missing protocol, skipping group",
            )
            continue

        raw_trait = str(doc.trait or canonical)
        key = (protocol, canonical)
        entry = grouped.setdefault(
            key,
            {
                "document_ids": [],
                "collection_paths": [],
                "image_prefixes": [],
            },
        )
        entry["document_ids"].append(doc.document_id)
        entry["collection_paths"].append(doc.collection_path)
        prefix = image_prefixes_by_document_id.get(doc.document_id)
        if prefix:
            entry["image_prefixes"].append(prefix)

    results: list[ProtocolTraitGroup] = []
    for (protocol, canonical), entry in grouped.items():
        image_prefixes = list(dict.fromkeys(entry["image_prefixes"]))
        results.append(
            ProtocolTraitGroup(
                protocol=protocol,
                raw_trait_value=canonical,
                canonical_trait_name=canonical,
                inference_trait_type=inference_trait_type(canonical),
                source_document_ids=list(entry["document_ids"]),
                source_collection_paths=list(entry["collection_paths"]),
                image_prefixes=image_prefixes,
            )
        )
    return results

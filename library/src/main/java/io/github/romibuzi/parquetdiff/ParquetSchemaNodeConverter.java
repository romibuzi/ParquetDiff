package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface ParquetSchemaNodeConverter {
    Logger LOGGER = LoggerFactory.getLogger(ParquetSchemaNodeConverter.class);

    static ParquetSchemaNode fromMessageType(MessageType messageType) {
        return new ParquetSchemaNode(ParquetSchemaType.MESSAGE.toString(),
                ParquetSchemaType.MESSAGE,
                null,
                null);
    }

    static ParquetSchemaNode fromGroupType(GroupType groupType) {
        return new ParquetSchemaNode(groupType.getName(),
                findGroupType(groupType),
                null,
                null);
    }

    static ParquetSchemaNode fromPrimitiveType(PrimitiveType primitiveType) {
        return new ParquetSchemaNode(primitiveType.getName(),
                ParquetSchemaType.PRIMITIVE,
                primitiveType.getPrimitiveTypeName(),
                primitiveType.getLogicalTypeAnnotation());
    }

    static ParquetSchemaType findGroupType(GroupType groupType) {
        LogicalTypeAnnotation logicalType = groupType.getLogicalTypeAnnotation();
        if (logicalType == null) {
            return ParquetSchemaType.GROUP;
        } else if (logicalType == LogicalTypeAnnotation.listType()) {
            return ParquetSchemaType.LIST;
        } else if (logicalType == LogicalTypeAnnotation.mapType()) {
            return ParquetSchemaType.MAP;
        }

        LOGGER.warn("Unrecognized Group logical type: {}", logicalType);
        return ParquetSchemaType.GROUP;
    }
}

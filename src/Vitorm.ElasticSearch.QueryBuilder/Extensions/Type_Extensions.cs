using System;
using System.Collections;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace Vitorm.ElasticSearch.QueryBuilder.Extensions
{
    public static partial class Type_Extensions
    {
        public static string GetColumn(this Type type, string propertyOrFieldName, out Type fieldType)
        {
            //#1 property
            var property = type.GetProperty(propertyOrFieldName, BindingFlags.Public | BindingFlags.Instance);
            if (property != null)
            {
                var name = (property.GetCustomAttributes(typeof(ColumnAttribute), true)?.FirstOrDefault() as ColumnAttribute)?.Name;
                if (name != null) propertyOrFieldName = name;

                fieldType = property.PropertyType;
                return propertyOrFieldName;
            }

            //#2 field
            var field = type.GetField(propertyOrFieldName, BindingFlags.Public | BindingFlags.Instance);
            if (field != null)
            {
                var name = (field.GetCustomAttributes(typeof(ColumnAttribute), true)?.FirstOrDefault() as ColumnAttribute)?.Name;
                if (name != null) propertyOrFieldName = name;

                fieldType = field.FieldType;
                return propertyOrFieldName;
            }

            fieldType = null;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="propertyOrFieldPath"> could be nested , example: "name"  "depart.name"  "departs[1].name" "departs.1.name"</param>
        /// <param name="fieldType"></param>
        /// <returns></returns>
        public static string GetNestedColumn(Type type, string propertyOrFieldPath, out Type fieldType)
        {
            if (string.IsNullOrWhiteSpace(propertyOrFieldPath) || type == null)
            {
                fieldType = null;
                return null;
            }

            propertyOrFieldPath = propertyOrFieldPath.Replace("]", "").Replace("[", ".");

            Type columnType = type;
            string columnPath = null;
            foreach (var fieldName in propertyOrFieldPath.Split('.'))
            {
                (columnPath, columnType) = GetColumnPath(columnPath, columnType, fieldName);
            }

            fieldType = columnType;
            return columnPath;
        }


        private static (string columnPath, Type columnType) GetColumnPath(string parentColumnPath, Type parentColumnType, string fieldName)
        {
            if (parentColumnType == null || fieldName == null) return default;

            if (parentColumnType.IsArray)
            {
                var columnType = parentColumnType.GetElementType();

                if (int.TryParse(fieldName, out var index))
                {
                    var column = $"[{index}]";
                    var columnPath = (parentColumnPath ?? "") + column;
                    return (columnPath, columnType);
                }
                else
                {
                    return GetColumnPath(parentColumnPath, columnType, fieldName);
                }
            }
            else if (parentColumnType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(parentColumnType))
            {
                var columnType = parentColumnType.GetGenericArguments().FirstOrDefault();

                if (int.TryParse(fieldName, out var index))
                {
                    var column = $"[{index}]";
                    var columnPath = (parentColumnPath ?? "") + column;
                    return (columnPath, columnType);
                }
                else
                {
                    return GetColumnPath(parentColumnPath, columnType, fieldName);
                }
            }
            else
            {
                var column = GetColumn(parentColumnType, fieldName, out var columnType);
                if (column == null) return default;

                var columnPath = parentColumnPath == null ? column : (parentColumnPath + "." + column);
                return (columnPath, columnType);
            }

        }

    }
}
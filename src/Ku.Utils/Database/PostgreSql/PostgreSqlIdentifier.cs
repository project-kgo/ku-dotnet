using System.Text.RegularExpressions;

namespace Ku.Utils.Database.PostgreSql;

internal static partial class PostgreSqlIdentifier
{
    public static string Quote(string identifier, string argumentName)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("PostgreSQL 标识符不能为空。", argumentName);
        }

        if (!ValidIdentifierRegex().IsMatch(identifier))
        {
            throw new ArgumentException("PostgreSQL 标识符只能包含字母、数字和下划线，并且不能以数字开头。", argumentName);
        }

        return $"\"{identifier}\"";
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex ValidIdentifierRegex();
}

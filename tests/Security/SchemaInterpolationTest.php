<?php

namespace DreamFactory\Core\Hana\Tests\Security;

use PHPUnit\Framework\TestCase;

/**
 * Security: HanaSchema must parameterize all schema/routine name lookups.
 *
 * Phase 2 audit found five interpolation sites:
 *   - loadTableColumns (SCHEMA_NAME / TABLE_NAME)
 *   - getTableConstraints (schema_name IN — twice, primary + referential)
 *   - getTableNames (SCHEMA_NAME)
 *   - getViewNames (schema_name — addslashes ineffective on HANA)
 *   - loadParameters (procname / schema_name)
 *
 * Fix: parameterize every site with named placeholders + a bindings array
 * passed to connection->select(), per the SQL driver template.
 */
class SchemaInterpolationTest extends TestCase
{
    private string $contents;

    protected function setUp(): void
    {
        $sourcePath = __DIR__ . '/../../src/Database/Schema/HanaSchema.php';
        $this->assertFileExists($sourcePath);
        $this->contents = file_get_contents($sourcePath);
    }

    public function testNoSchemaInterpolation(): void
    {
        $this->assertDoesNotMatchRegularExpression(
            "/'\{?\\\$schema\}?'/",
            $this->contents,
            'No \$schema must be interpolated single-quoted into SQL'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/'\{?\\\$tableName\}?'/",
            $this->contents,
            'No \$tableName must be interpolated single-quoted into SQL'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/'\{?\\\$holder->\w+\}?'/",
            $this->contents,
            'No \$holder->resourceName / \$holder->schemaName must be interpolated'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/'\{\\\$safeSchema\}'/",
            $this->contents,
            'addslashes-then-interpolate pattern must not remain'
        );
    }

    public function testNamedPlaceholdersUsed(): void
    {
        $this->assertMatchesRegularExpression(
            '/:schema\b/',
            $this->contents,
            ':schema named placeholder must be present'
        );
        $this->assertMatchesRegularExpression(
            '/:name\b/',
            $this->contents,
            ':name named placeholder must be present'
        );
    }

    public function testInClauseUsesPlaceholderArray(): void
    {
        $this->assertMatchesRegularExpression(
            '/array_fill\s*\(\s*0\s*,\s*count\s*\(\s*\$schemas\s*\)/',
            $this->contents,
            'Multi-schema IN(...) clause must use array_fill placeholder list'
        );
    }
}

<?php
namespace Test;

$path = array(
    __DIR__ . '/../library',
    get_include_path()
);
set_include_path( implode( PATH_SEPARATOR, $path ) );

require_once 'SQL/Maker.php';

/**
 * Test class for SQL\Maker.
 */
class SQLMakerTest extends \PHPUnit_Framework_TestCase
{
    protected $object;
    protected function setUp()
    {
        $this->object = new \SQL\Maker;
    }

    protected function tearDown()
    {
    }

    public function testInsert()
    {
        list( $sql, $params ) = $this->object->insert(
            'user',
            array( 'id' => 1, 'name' => 'sample' )
        );

        $this->assertEquals(
            'INSERT INTO `user` ( `id`,`name` ) VALUES ( ?,? )',
            $sql
        );

        $this->assertEquals( array( 1, 'sample' ), $params );
    }

    public function testBulkInsert()
    {
        list( $sql, $params ) = $this->object->bulk_insert(
            'user',
            array( array( 'id' => 1, 'name' => 'sample1' )
                   , array( 'id' => 2, 'name' => 'sample2' )
                   , array( 'id' => 3, 'name' => 'sample3' )
            )
        );

        $this->assertEquals(
            'INSERT INTO `user` ( `id`,`name` ) VALUES ( ?,? ),( ?,? ),( ?,? )',
            $sql
        );

        $this->assertEquals( array( 1, 'sample1', 2, 'sample2', 3, 'sample3' ), $params );

        try {
            $this->object->bulk_insert( 'table1', 'foo' );
            $this->fail();
        } catch ( \Exception $e ) {
            $this->assertEquals( 'Invalid argument', $e->getMessage() );
        }

        try {
            $this->object->bulk_insert( 'table1', array() );
            $this->fail();
        } catch ( \Exception $e ) {
            $this->assertEquals( 'Invalid argument', $e->getMessage() );
        }

        try {
            $this->object->bulk_insert( 'table1', array( 'foo' ) );
            $this->fail();
        } catch ( \Exception $e ) {
            $this->assertEquals( 'Unknown type', $e->getMessage() );
        }

        try {
            $this->object->bulk_insert( 'table1',
                                        array( array( 'col1' => 'val' ),
                                               array( 'col1' => 'val', 'col2' => 'val' ) ) );
            $this->fail();
        } catch ( \Exception $e ) {
            $this->assertEquals( 'Invalid argument', $e->getMessage() );
        }
    }

    public function testUpsert()
    {
        list( $sql, $params ) = $this->object->upsert(
            'user',
            array( 'id' => 1, 'name' => 'sample' )
        );

        $this->assertEquals(
            'INSERT INTO `user` ( `id`,`name` ) VALUES ( ?,? ) ON DUPLICATE KEY UPDATE `id` = VALUES(`id`), `name` = VALUES(`name`)',
            $sql
        );

        $this->assertEquals( array( 1, 'sample' ), $params );
    }

    public function testUpsertWithOp()
    {
        list( $sql, $params ) = $this->object->upsertWithOp(
            'user',
            array( 'id' => 1, 'count' => '1', 'pt' => '2' ),
            array( 'count' => '+10', 'pt' => '-2' )
        );

        $this->assertEquals(
            'INSERT INTO `user` ( `id`,`count`,`pt` ) VALUES ( ?,?,? ) ON DUPLICATE KEY UPDATE `count` = `count` +10, `pt` = `pt` -2',
            $sql
        );

        $this->assertEquals( array( 1, 1, 2 ), $params );
    }

    public function testUpdate()
    {
        list( $sql1, $params1 ) = $this->object->update(
            'user',
            array( 'name' => 'dummy', 'email' => 'dummy@sample.com' )
        );

        $this->assertEquals(
            'UPDATE `user` SET `name` = ?,`email` = ?',
            $sql1
        );

        $this->assertEquals( array( 'dummy', 'dummy@sample.com' ), $params1 );
    }

    public function testSelect()
    {
        list( $sql1, $params1 ) = $this->object->select( 'user' );
        list( $sql2, $params2 ) = $this->object->select( 'user', array( 'id', 'name' ) );
        list( $sql3 ) = $this->object->select(
            'employee',
            array(
                'name',
                array( 'count' => 'employee_id' ),
                array( 'max' => array( 'length' => 'name' ), '-as' => 'longest_name' )
            )
        );
        list( $sql4 ) = $this->object->select(
            array(
                'user' => array(
                    '-inner' => array( 'item', 'i' ),
                    '-on'    => array( 'id', '`i`.`user_id`' )
                )
            )
        );

        list( $sql5 ) = $this->object->select(
            array(
                'user' => array(
                    array(
                        '-inner' => array( 'item', 'i' ),
                        '-on'    => array( 'id', '`i`.`user_id`' )
                    ),
                    array(
                        '-inner' => array( 'collection', 'c' ),
                        '-on'    => array( 'id', '`c`.`id`' )
                    )
                )
            )
        );

        list( $sql6 ) = $this->object->select(
            'employee',
            array(
                'name' => array( '-as' => 'alias' )
            )
        );

        $this->assertEquals(
            'SELECT * FROM `user` `me`',
            $sql1
        );

        $this->assertEquals( array(), $params1 );

        $this->assertEquals(
            'SELECT `me`.`id`,`me`.`name` FROM `user` `me`',
            $sql2
        );

        $this->assertEquals( array(), $params2 );

        $this->assertEquals(
            'SELECT `me`.`name`,COUNT( `me`.`employee_id` ),MAX( LENGTH( `me`.`name` ) ) AS `longest_name` FROM `employee` `me`',
            $sql3
        );

        $this->assertEquals(
            'SELECT * FROM `user` `me` INNER JOIN `item` `i` ON `me`.`id` = `i`.`user_id`',
            $sql4
        );

        $this->assertEquals(
            'SELECT * FROM `user` `me` INNER JOIN `item` `i` ON `me`.`id` = `i`.`user_id` INNER JOIN `collection` `c` ON `me`.`id` = `c`.`id`',
            $sql5
        );

        $this->assertEquals(
            'SELECT `me`.`name` AS `alias` FROM `employee` `me`',
            $sql6
        );
    }

    public function testDelete()
    {
        list( $sql1, $params1 ) = $this->object->delete( 'user' );

        $this->assertEquals(
            'DELETE FROM `user`',
            $sql1
        );

        $this->assertEquals( array(), $params1 );
    }

    public function testReplace()
    {
        list( $sql, $params ) = $this->object->replace(
            'user',
            array( 'id' => 1, 'name' => 'sample' )
        );

        $this->assertEquals(
            'REPLACE INTO `user` ( `id`,`name` ) VALUES ( ?,? )',
            $sql
        );

        $this->assertEquals( array( 1, 'sample' ), $params );
    }

    public function testWhere()
    {
        list( $sql1, $params1 ) = $this->object->where(
            array(
                'id'    => array( '<' => 100 ),
                'name'  => 'dummy',
                'email' => 'dummy@sample.com'
            )
        );
        list( $sql2, $params2 ) = $this->object->where(
            array( 'id', 'name', 'email' )
        );
        list( $sql3 ) = $this->object->where(
            array(),
            array( 'orderBy' => array(
                'id',
                array( '-desc' => 'name' )
            ) )
        );
        list( $sql4 ) = $this->object->where(
            array(),
            array( 'rows' => 5 )
        );
        list( $sql5 ) = $this->object->where(
            array(),
            array(
                'rows'   => 5,
                'offset' => 10
            )
        );
        list( $sql6, $params6 ) = $this->object->where(
            array( '-or' => array(
                array( 'source_id' => 1 ),
                array( 'dest_id' => 1 )
            ) )
        );
        list( $sql7 ) = $this->object->where(
            array(),
            array(
                'groupBy' => array( 'book', 'chapter' )
            )
        );
        list( $sql8 ) = $this->object->where(
            array(),
            array( 'orderBy' => array(
                '`foreign`.`id`',
                array( '-desc' => '`foreign`.`name`' )
            ) )
        );
        list( $sql9, $params9 ) = $this->object->where(
            array( 'id'=> array( '-in' => array( 1, 2, 3 ) ) )
        );

        $this->assertEquals(
            ' WHERE ( `id` < ? AND `name` = ? AND `email` = ? )',
            $sql1
        );

        $this->assertEquals( array( 100, 'dummy', 'dummy@sample.com' ), $params1 );

        $this->assertEquals(
            ' WHERE ( `id` = ? AND `name` = ? AND `email` = ? )',
            $sql2
        );

        $this->assertEquals( array(), $params2 );

        $this->assertEquals(
            ' ORDER BY `me`.`id`,`me`.`name` DESC',
            $sql3
        );

        $this->assertEquals(
            ' LIMIT 5',
            $sql4
        );

        $this->assertEquals(
            ' LIMIT 5 OFFSET 10',
            $sql5
        );

        $this->assertEquals(
            ' WHERE ( ( ( `source_id` = ? ) OR ( `dest_id` = ? ) ) )',
            $sql6
        );

        $this->assertEquals(
            array( 1, 1 ),
            $params6
        );

        $this->assertEquals(
            ' GROUP BY `me`.`book`,`me`.`chapter`',
            $sql7
        );

        $this->assertEquals(
            ' ORDER BY `foreign`.`id`,`foreign`.`name` DESC',
            $sql8
        );

        $this->assertEquals(
            ' WHERE ( `id` IN ( ?,?,? ) )',
            $sql9
        );

        $this->assertEquals( array( 1, 2, 3 ), $params9 );

    }
}

		<?php if ( ! defined('BASEPATH')) exit('No direct script access allowed');
/*
|	Author: 	Eric Roberts
|	Version:	1.0
|	Updated:	2019-06-19
|
*/
class Singleton extends DMSA {

	public static $DB;

	public static function Assume( $task, $maxDecay=15, $interval=5 ){
		self::Init();

		$class = get_called_class();

		if( !self::Claim( $task ) ){
			$current = self::Retrieve( $task );
			if( time() - strtotime( $current->last_ts ) >= $maxDecay ){
				self::Claim( $task, true );
			}else return false;
		}

		return new $class( $task, $maxDecay, $interval );
	}

	public static function Retrieve( $task ){
		return self::$DB->query("SELECT * FROM singleton_data WHERE task='{$task}' LIMIT 0, 1")->row();
	}

	public static function Claim( $task, $force=false ){
		$data = null;
		$pid = getmypid();
		$start_ts = date('Y-m-d H:i:s');
		$last_ts = $start_ts;
		$end_ts = null;
		$insert_data = compact( 'task', 'pid', 'start_ts', 'last_ts', 'end_ts', 'data' );
		$insert_string = $force
		 	?	self::ForceInsertString( 'singleton_data', $insert_data, ['pid','start_ts','data','last_ts'] )
			:	self::ForceInsertString( 'singleton_data', $insert_data, [ 'task' ] );

		$result = self::$DB->query( $insert_string );

		return self::$DB->affected_rows() > 0;
	}

	protected static function ForceInsertString( $table, $kvPairs, $onDupe=[] ){
		$onDupe = $onDupe===true || empty( $onDupe ) ? [ key( $kvPairs ) ] : $onDupe;
		$base = self::$DB->insert_string( $table, $kvPairs );
		$temp = [];

		foreach( $onDupe as $k => $v ){
			list( $key, $val ) = is_numeric( $k )
				?	[ $v, self::$DB->escape( $kvPairs[ $v ] ) ]
				:	[ $k, self::$DB->escape( $v ) ];
			$temp[] = "{$key}={$val}";
		}

		$updates = implode( ',', $temp );

		return "{$base} ON DUPLICATE KEY UPDATE {$updates}";
	}

	protected static function Init(){
		if( !isset( self::$DB ) ){
			$ci =& get_instance();
			self::$DB = $ci->db;
		}
	}

	public $pid;
	public $task;
	public $start_ts;
	public $last_ts;
	public $end_ts;
	public $data;

	public $update_interval = 5;
	public $next_update_ts;
	public $abrupt_end;
	public $engaged;
	public $finished;
	public $self_cleaning;

	public function __destruct(){
		if( !$this->abrupt_end ){
			$now = date('Y-m-d H:i:s');
			self::$DB->query("UPDATE singleton_data SET end_ts='{$now}' WHERE task='{$this->task}' AND pid={$this->pid}");
		}
	}

	protected function __construct( $task, $maxDecay, $interval ){
		$this->interval = $interval;
		$this->data = new stdClass;
		$this->pid = getmypid();
		$this->task = $task;
		$this->engaged = 1;
		$this->update();
	}

	public function bind( $key, &$value ){ return $this->data->$key =& $value; }

	public function unbind( $key ){ unset( $this->data->$key ); }

	public function update( $force=false ){
		if( $force || $this->time_to_update() ){
			$last_ts = date('Y-m-d H:i:s');
			$data = str_replace( '[]','null', json_encode( $this->data ));

			$result = self::$DB->query(self::$DB->update_string(
				'singleton_data',
				compact( 'last_ts', 'data' ),
				"task='{$this->task}' AND pid={$this->pid}"
			));

			return $result && self::$DB->affected_rows()
				?	$this->next_update_ts = time() + $this->update_interval
				:	$this->drop_out();
		}
	}

	public function finish( $cleanup=false ){
		if( !$this->finished ){
			if( $cleanup || $this->self_cleaning ){
				$result = $this->cleanup();
			}elseif( !$this->finished ){

				$end_ts = date('Y-m-d H:i:s');
				$data = str_replace( '[]','null', json_encode( $this->data ));

				$result = self::$DB->query(self::$DB->update_string(
					'singleton_data',
					compact( 'end_ts', 'data' ),
					"task='{$this->task}' AND pid={$this->pid}"
				));
			}

			$this->finished = 1;
		}

		return $result;
	}

	public function cleanup(){
		return self::$DB->query("DELETE FROM singleton_data WHERE task='{$this->task}' AND pid={$this->pid}");
	}

	protected function drop_out(){
		$abrupt_end = true;
		$this->engaged = false;
	}

	protected function time_to_update(){
		return is_null( $this->next_update_ts ) || time() >= $this->next_update_ts;
	}

}

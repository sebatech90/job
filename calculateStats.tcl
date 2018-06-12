package provide RealtimeSchedulerStats2 0.1

package provide RealtimeSchedulerDelta 0.1

package require dicttool

namespace eval rts_stats {

   variable statesColours
	variable otherNames
	variable ordersPackageInfo
   variable productsRouteInfo

	set ordersPackageInfo {}
   set productsRouteInfo {}

   set statesColours {
		orders_time #90ed7d
		setup_time #FF69B4
		other_time #d3d3d3
	}
   
   set otherNames {
		PERIODIC "Zdarzenia cykliczne" 
		RAMP_DOWN PRODUCTION_PLANNING_RAMP_DOWN_PROMPT
		RAMP_UP PRODUCTION_PLANNING_RAMP_UP_PROMPT
		PLANNED_DOWNTIME PRODUCTION_PLANNING_EXTERNAL_PROMPT 
		STATIC "Zdarzenia niecykliczne"
	}
}

proc ::rts_stats::freezeVisibleLinesStats {session} {
   if {[antIsDebug]} {antLog debug [info level 0]}
	
   rts_stats::onTableData $session [dict create tabId 3]
   setSessionData $session DataServer completeTargetStatsFreezed [getSessionData $session DataServer completeTargetStats]
}

#::rts_stats::getFactors $sourceId $lineId $from $to $orders_time $setup_time $sumOther $OEE_factor $OEE_FACTOR_1 $OEE_FACTOR_2_SUM $sum_target $sum_target_op
proc ::rts_stats::getFactors {selectedSrc selectedLine from to orders_time setup_time sumOther OEE_factor OEE_FACTOR_1 OEE_FACTOR_2 sum_target sum_target_op} {
   antLog debug [info level 0]
   
   #antLog statsCompileOee [format %.2f [expr 100.00 * $OEE_FACTOR_1 / $OEE_FACTOR_2]]

   set query "
   DELETE FROM Planning.PlanStatistics where source_id = $selectedSrc AND equipment_id = (select id from Equipment.Equipment where tag = '$selectedLine') and planned_start='[antTimeToString $from 1 0]' and planned_end='[antTimeToString $to 1 0]';
   INSERT INTO Planning.PlanStatistics (source_id,equipment_id ,planned_start ,planned_end,data ,update_time)
   VALUES (
      $selectedSrc,
      (select id from Equipment.Equipment where tag = '$selectedLine'),
      '[antTimeToString $from 1 0]','[antTimeToString $to 1 0]',
      'TARGET $sum_target TARGET_OP $sum_target_op OEE $OEE_factor OEE_factor_1 $OEE_FACTOR_1 OEE_factor_2 $OEE_FACTOR_2 OTHERS $sumOther ORDERS_TIME $orders_time CO_TIME $setup_time',
      GETUTCDATE())"
#   antLog q $query
   utils::runSql $query
}

proc ::rts_stats::onTableData {session data} {
	 try {
		if {[antIsDebug]} {antLog debug [info level 0]}

		if {[dict get $data tabId] == 3} {
		
			DropdownStats.enable $session

			if {[getSessionData $session DataServer selectedRowId] ne {}} {
				set selectedLine [getSessionData $session DataServer selectedRowId]
			} else {
				set selectedLine "0_B_43"
			}

			set src [lindex [split $selectedLine "_"] 0]
			set from [dict get [DatePicker.getValue $session] dateValueFrom]
			set to [dict get [DatePicker.getValue $session] dateValueTo]
			set current_time [clock seconds]

			dict set stats $selectedLine [::rts_stats::calculateSummary $selectedLine $from $to $current_time $session]
			::rts_stats::setPageControls [dict get $stats $selectedLine] $selectedLine $current_time $session

         #Show target progress
         #variable completeTarget
         
			if {[LineListbox.getValue $session] ne {}} {
				set visibleLines [split [LineListbox.getValue $session] ","]
			} else {
				set visibleLines [dict keys [dict get $::EquipmentMap [AreaListbox.getValue $session]]]
			}
			
			set sumVisLinesTarget 0
			set sumVisLinesTargetOp 0
			
			foreach displayedLine $visibleLines {
				set srcLineName [join [list $src $displayedLine] "_"]
				dict set stats $srcLineName [::rts_stats::calculateSummary $srcLineName $from $to $current_time $session]
				
				set sumVisLinesTarget [expr $sumVisLinesTarget + [dict get $stats $srcLineName sum_target]]
				set sumVisLinesTargetOp [expr $sumVisLinesTargetOp + [dict get $stats $srcLineName sum_target_op]]
				#antLog newStats "[dict get $stats $displayedLine] for $displayedLine"
			}

         setSessionData $session DataServer completeTargetStats [list sumVisLinesTarget $sumVisLinesTarget sumVisLinesTargetOp $sumVisLinesTargetOp]
			
			setPageProfit $session
			
		} else {
			DropdownStats.disable $session
		}
   } on error {msg opt} {
      antLog error $msg
   } finally {

   }
}

proc ::rts_stats::calculateSummary {selectedLine from to current_time session {fromSession 1}} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	set stats {}
	
	set factorKeys {
		orders_time setup_time other_time orders_time_future setup_time_future \
		sum_production sum_production_op \
		sum_target sum_target_op \
		orders_cnt setup_cnt \
		OEE_factor OEE_FACTOR_1 OEE_FACTOR_2
	}
	foreach key $factorKeys {dict set stats $key 0}
	
	dict set stats producedChart {}
	dict set stats targetChart {}
	dict set stats otherStatsDetails {}
	dict set stats hisDetails {}
	
	variable ordersPackageInfo
	variable productsRouteInfo
	
	if {$ordersPackageInfo eq {}} {
		set ordersPackageInfo [::rts_stats::getOrdersPackageDetails]
	}

	if {$productsRouteInfo eq {}} {
		set productsRouteInfo [::rts_stats::getProductsRouteDetails]
	}

	if {$from < $current_time} {
		if {$current_time < $to} {
         set endHistory $current_time
         set dataRes "OEE_Hour"
      } else {
         set endHistory $to
         #Check data range
         if {[expr $to - $from] > 604800} {set dataRes "OEE_Shift"} {set dataRes "OEE_Hour"}
      }
		set stats [updateStatsHistoryData $stats $selectedLine $from $endHistory $dataRes]
		dict set stats hisDetails [getHistoryOrdersDetails $selectedLine $from $endHistory]
	}
#	antLog statsHis $stats
#	antLog currTime $current_time
	set timelineData [rts_stats::getTimeLineData $selectedLine $from $to $current_time $fromSession $session]
	if {$session ne {}} {set area [getSessionData $session DataServer area]} {set area "PACKAGING"}
	
	set stats [::rts_stats::updateStatsCachedData $stats $from $to $current_time $timelineData $area]
	#antLog statsFuture $stats
	dict set stats units [::rts_stats::getUnit $selectedLine]
	return $stats
}

proc ::rts_stats::updateStatsHistoryData {stats placeId timeFrom timeTo dataResolution} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	set lineId [join [lrange [split $placeId _] 1 end] "_"]

	set query "
	SELECT DATEDIFF(HOUR,'[antTimeToString $timeFrom 1 0]',time_from) d,time_from,data from mes_lines_statistics mls
	INNER JOIN Equipment.Equipment ee on place_id = ee.id
	WHERE mls.name = '$dataResolution' and ee.tag = '$lineId' AND 
	mls.time_from >= '[antTimeToString $timeFrom 1 0]' AND mls.time_to <= '[antTimeToString $timeTo 1 0]' order by mls.time_from;"

	#antLog q $query
	set OEE_data {}
	
	if {$lineId in {APN G_01 G_02 D_25 D-25 D_20 D-20}} {return $stats}
	
	if {$lineId in {T_37 T_137 T_121 T_35}} {set scaleDivide 1000} {set scaleDivide 1}
	
	#MANUFACTURING
	#[list APN APN G_01 G-01 G_02 G-02 T_35 T-35 T_37 T-37 T_121 T-121 T_137 T-137 D_25 D-25 D_20 D-20]

	foreach row [utils::runSql $query] {
		foreach { d from data} $row {
		
			set realProduction [dict get $data PRODUCTION]
			set realPacks [dict get $data PACKS]
			#set realTarget [dict get $data PRODUCTION_EXPECTED_REAL]
			set realSetup [expr [dict get $data SET_UP] + [dict get $data SET_UP_OVERTIME]]
			set realRuntime [expr [dict get $data RUNTIME] + ([dict get $data DOWNTIME] - $realSetup)]
			set otherStatesTime [dict get $data EXTERNAL_TIME]
			
			dict lappend stats producedChart [list [dict get $stats sum_production] [antStringToTime $from 1] {} ""]
			#dict lappend stats targetChart [list [dict get $stats sum_target] [antStringToTime $from 1] {} ""]
			lappend OEE_data $data
			
			dict set stats sum_production [expr [dict get $stats sum_production] + [expr 1.00 * $realProduction / $scaleDivide]]
			dict set stats sum_production_op [expr [dict get $stats sum_production_op] + $realPacks]
			#dict set stats sum_target [expr [dict get $stats sum_target] + $realTarget]
			dict set stats orders_time [expr [dict get $stats orders_time] + $realRuntime]
			dict set stats other_time [expr [dict get $stats other_time] + $otherStatesTime]
			dict set stats setup_time [expr [dict get $stats setup_time] + $realSetup]
			
			dict set stats OEE_FACTOR_1 [expr [dict get $stats OEE_FACTOR_1] + [dict get $data EFFICIENT_RUNTIME]]
			dict set stats OEE_FACTOR_2 [expr [dict get $stats OEE_FACTOR_2] + [dict get $data WORK_TIME]]
		}
	}
	
	dict set stats OEE_factor [dict get [mes_stats::calculateIndex "" "OEE" [mes_stats::compileData "" "OEE" $OEE_data]] OEE]
	return $stats
}

proc getHistoryOrdersDetails {placeId timeFrom timeTo} {

	set lineId [join [lrange [split $placeId _] 1 end] "_"]

	set query "
	WITH cte1 AS (
		SELECT mes_orders.*, ROW_NUMBER() OVER (PARTITION BY place_id ORDER BY planned_finish DESC) AS rowNumber FROM mes_orders 
		INNER JOIN Equipment.Equipment ee ON place_id = ee.id 
		WHERE planned_finish > '[antTimeToString $timeFrom 1 0]' and planned_start < '[antTimeToString $timeTo 1 0]' and status = 'produkcja zakończona' and ee.tag = '$lineId'
	)

	select vt.place_id, vt.order_number, vt.ext_time, sum(datediff(ss, time_on_order, time_off_order)) vis_ext_time from (

		SELECT * FROM (
			SELECT place_id,
					order_number,
					planned_start,
					planned_finish,
					planned_start_vis = (case when planned_start < '[antTimeToString $timeFrom 1 0]' AND planned_finish > '[antTimeToString $timeFrom 1 0]' THEN '[antTimeToString $timeFrom 1 0]' ELSE planned_start END),
					planned_finish_vis = (case when planned_start < '[antTimeToString $timeTo 1 0]' AND planned_finish > '[antTimeToString $timeTo 1 0]' THEN '[antTimeToString $timeTo 1 0]' ELSE planned_finish END),
					datediff(ss, planned_start, planned_finish) order_full
			FROM cte1
		) T1
		OUTER APPLY (
			SELECT 
			sum(datediff(ss, time_on, time_off)) ext_time
			FROM mes_states ms
			WHERE ms.time_off >= t1.planned_start AND ms.time_on <= t1.planned_finish AND ms.place_id = t1.place_id AND ms.state_id IN (3)
		) T2
		OUTER APPLY (
			SELECT 
				time_on_order = (case when time_on < '[antTimeToString $timeFrom 1 0]' AND time_off > '[antTimeToString $timeFrom 1 0]' THEN '[antTimeToString $timeFrom 1 0]' ELSE time_on END),
				time_off_order = (case when time_on < '[antTimeToString $timeTo 1 0]' AND time_off > '[antTimeToString $timeTo 1 0]' THEN '[antTimeToString $timeTo 1 0]' ELSE time_off END)
			FROM mes_states ms
			WHERE ms.time_off > t1.planned_start_vis AND ms.time_on < t1.planned_finish_vis AND ms.place_id = t1.place_id AND ms.state_id IN (3)
		) T3

	) vt
	group by vt.place_id, vt.order_number, vt.ext_time"
	#antLog q $query
	set hisOrdersDetails {}
	foreach row [utils::runSql $query] {
		foreach {place_id order_number ext_time vis_ext_time} $row {
		
			if {$ext_time eq {}} {set ext_time 0}
			if {$vis_ext_time eq {}} {set vis_ext_time 0}
		
			dict set hisOrdersDetails $order_number [dict create extTime $ext_time visExtTime $vis_ext_time]
		}
	}
	return $hisOrdersDetails
}

proc ::rts_stats::getOrdersPackageDetails {} {

	set query "
	SELECT item_tag, value FROM Items.ItemPropertyView2 WHERE property_tag = 'BLISTERS'"
	
	#antLog qPac $query
	set packageInfoOrders {}
	foreach row [::utils::runSql $query] {
		foreach {item_tag elements_in_pack} $row {
			dict set packageInfoOrders $item_tag $elements_in_pack
		}
	}
	return $packageInfoOrders
}

proc ::rts_stats::getProductsRouteDetails {} {

	set query "
	SELECT item_tag, item_name, equipment_id,  ee.tag, cycle_time_s, efficiency FROM
	Items.RoutingDefinitionToItemView INNER JOIN Equipment.Equipment ee on equipment_id = ee.id
	WHERE item_tag IS NOT NULL AND cycle_time_s IS NOT NULL AND efficiency IS NOT NULL"
	
	#antLog qPac $query
	set productsInfoRoute {}
	foreach row [::utils::runSql $query] {
		foreach {item_tag item_name equipment_id tag cycle_time_s efficiency} $row {
			dict set productsInfoRoute $item_tag $tag CT $cycle_time_s
			dict set productsInfoRoute $item_tag $tag EF $efficiency
		}
	}
	return $productsInfoRoute
}

proc rts_stats::getTimeLineData {line from to current_time fromSession session} {

	if {$fromSession} {
      
      antLog unforceCache unforceCache
		#Changeovers planned
		dict set timelineData changeoversEvents {}
		#External times planned
		dict set timelineData breaksData {}
		#Changeovers history
		dict set timelineData downTimeDataChangeover {}
		#External times history
		dict set timelineData downTimeDataBreaks {}
		#Planned orders
		dict set timelineData ordersEvents {}
		
		if {[dict exists [getSessionData $session DataServer breaksData] $line]} {
			dict set timelineData breaksData [dict get [getSessionData $session DataServer breaksData] $line]
		}

		if {[dict exists [getSessionData $session DataServer downTimeDataChangeover] $line]} {
			dict set timelineData downTimeDataChangeover [dict get [getSessionData $session DataServer downTimeDataChangeover] $line]
		}

		if {[dict exists [getSessionData $session DataServer downTimeDataBreaks] $line]} {
			dict set timelineData downTimeDataBreaks [dict get [getSessionData $session DataServer downTimeDataBreaks] $line]
		}

		if {[dict exists [getSessionData $session DataServer ordersData] $line]} {
			dict set timelineData ordersEvents [dict get [getSessionData $session DataServer ordersData] $line]
		}
		
		if {[dict exists [getSessionData $session DataServer changeoversData] $line]} {
			dict set timelineData changeoversEvents [dict get [getSessionData $session DataServer changeoversData] $line]  
		}
	} else {
      #antLog forceCache forceCache
      
		set sourceId [lindex [split $line _] 0]
		set equipmentTag [join [lrange [split $line _] 1 end] _]
	
		lassign [planning::getStackData $sourceId $equipmentTag $from $to] ordersData changeoversData
		
		#antLog getStackData getStackData
		
		dict set timelineData breaksData [dict getnull [planning::getExternalTimesDelta $sourceId $equipmentTag $from $to $current_time] $line]
		
		#antLog getStackData getStackData2
		
		dict set timelineData downTimeDataChangeover [dict getnull [planning::getDowntimeDataChangeover 0 $equipmentTag $from $to] $line]
		
		#antLog getStackData getStackData3
		
		dict set timelineData downTimeDataBreaks [dict getnull [planning::getDowntimeData 0 $equipmentTag $from $to] $line]
		
		#antLog getStackData getStackData4
		
		dict set timelineData ordersEvents [dict getnull $ordersData $line]
		
		#antLog getStackData getStackData5
		
		dict set timelineData changeoversEvents [dict getnull $changeoversData $line]
		
		#antLog getStackData getStackData6

	}
	return $timelineData
}

proc ::rts_stats::updateStatsCachedData {stats from to current_time cachedData area} {
	if {[antIsDebug]} {antLog debug [info level 0]}

	set OTHER {}
	set OTHER_CO {}
	set CLIPBOARD {}
	
	set changeoversEvents [dict get $cachedData changeoversEvents]
	#External times planned
	set breaksData [dict get $cachedData breaksData]
	#Changeovers history
	set downTimeDataChangeover [dict get $cachedData downTimeDataChangeover]
	#External times history
	set downTimeDataBreaks [dict get $cachedData downTimeDataBreaks]
	#Planned orders
	set ordersEvents [dict get $cachedData ordersEvents]

	if {$area eq "MANUFACTURING"} {
		set mergedWorkTime [mergeEventsTime $ordersEvents $breaksData $from $to $current_time]
	}
	
	set Events [::rts_stats::createAndSortEvents $ordersEvents $changeoversEvents $breaksData $from $to $current_time]

	set visible_end {}
	set prevOrder {}
	
   foreach singleEvent $Events {
		#antLog ev [dict print $singleEvent]
		unset -nocomplain event_type order_tag item_name additionals

		dict with singleEvent {

			switch $event_type {
				1 - 2 {
				#Orders
            #antLog order $singleEvent
					if {$area eq "MANUFACTURING"} {
                  #antLog singleEventMan [dict print $singleEvent]
						set stats [::rts_stats::calcOrderMan $stats $singleEvent $from $to $current_time]
						
						if {$planned_end > $to} {set visible_end $to} {set visible_end $planned_end}
						set lastOrderManVisEnd $visible_end
					} else {
						set stats [::rts_stats::calcOrderPac $stats $singleEvent $from $to $current_time]
					}
					dict incr stats orders_cnt
				}
				
				3 - 4 - 5 - 6 {
				#Other states
					dict lappend CLIPBOARD $event_type [list from $planned_start to $planned_end name [dict get $data NAME]]
					#antLog otherStatesSingleEvent $singleEvent
				}
				
				"STATS_CO_EVENT" {
				#Co in progress or planned
					if {[dict exists $singleEvent additionals EXTERNAL_TIME]} {

						set extEvents [lsort -integer -index 3 [dict getnull $singleEvent additionals EXTERNAL_TIME]]
						set extEventsSorted [lsort -integer -index 3 $extEvents]
						set extTimesInView [lsort -integer -index 3 [::rts_stats::getViewSessionExternal $extEvents $from $to]]
						
						set prevTs $planned_start
						set coTs 0
					
						foreach externalEvent $extTimesInView {
						
							set extFrom [dict get $externalEvent from]
							set extTo [dict get $externalEvent to]
							if {$extFrom < $current_time && $from < $current_time} {set extFrom $current_time}
						
							if {$extTo > $prevTs} {
								set coTs [expr $coTs + $extFrom - $prevTs]
								set prevTs $extTo
							}
						}
					} else {
						set coTs [expr $planned_end - $planned_start]
					}
					
					dict set stats setup_time [expr [dict get $stats setup_time] + $coTs]
					dict set stats setup_time_future [expr [dict get $stats setup_time_future] + $coTs]
					dict incr stats setup_cnt
				}
				
				"STATS_BREAK_EVENT" {
					dict lappend OTHER PLANNED_DOWNTIME [list from $planned_start to $planned_end]
				}
				
				default {
					antLog error "Event without event_type key"
				}
			}
      }
	}
	#End events loop
	
	if {$area eq "MANUFACTURING"} {
		dict set stats orders_time [expr [dict get $stats orders_time] + $mergedWorkTime]
		dict lappend stats targetChart [list [dict get $stats sum_target] $visible_end {} {}]
	}

	if {$visible_end <= $to} {
		if {[dict get $stats sum_target] == 0} {dict lappend stats targetChart [list [dict get $stats sum_target] $from {} {}]}
		dict lappend stats targetChart [list [dict get $stats sum_target] $to {} {}]
	}

	#Co history
	foreach coEvent $downTimeDataChangeover {
		if {[dict get $coEvent start_time] > $to || [dict get $coEvent end_time] < $from} {continue}
		dict incr stats setup_cnt
	}

	dict set stats otherStatsDetails [::rts_stats::calcOtherDetails [dict create OTHER $OTHER OTHER_CO $OTHER_CO CLIPBOARD $CLIPBOARD] [dict get $stats other_time]]
	return $stats
}

proc ::rts_stats::createAndSortEvents {orders changeovers breaks from to current_time} {
	set eventsList [concat $orders $changeovers $breaks]
	array set eventArray {}

	foreach singleEvent $eventsList {
	
		set evFrom [dict get $singleEvent planned_start]
		set evTo [dict get $singleEvent planned_end]
	
		if {$evFrom < $to && $evTo > $from} {

			if {[dict exists $singleEvent order_tag]} {
				if {[string match "*_C*" [dict get $singleEvent order_tag]]} {
					set evType STATS_CO_EVENT	
				} else {
					set evType [dict get $singleEvent event_type]	
				}
			} else {
				set evType STATS_BREAK_EVENT
			}
			
			switch $evType {
				STATS_CO_EVENT -
				STATS_BREAK_EVENT
				{
					if {$evTo < $current_time} {continue}
					if {$evFrom < $from} {set evFrom $from}
					if {$evTo > $to} {set evTo $to}
					if {$evFrom < $current_time && $evTo > $current_time} {set evFrom $current_time}
				}
				3 - 4 - 5 - 6 {
					if {$evFrom < $from} {set evFrom $from}
					if {$evTo > $to} {set evTo $to}
				}
				default {}
			}
			
			dict set singleEvent event_type $evType
			dict set singleEvent planned_start $evFrom
			dict set singleEvent planned_end $evTo		
			set eventArray($evFrom) $singleEvent
		}
	}
	
	set compiledEvents {}
	foreach key_from [lsort [array names eventArray]] {
		lappend compiledEvents $eventArray($key_from)
	}
	
	#antLog compiledEvents $compiledEvents
	return $compiledEvents
}

proc ::rts_stats::mergeEventsTime {ordersEvents breaks from to current_time} {
	set events [concat $ordersEvents $breaks]
	set states {}
	set sortedWorkStates {}
	set breaksEvents {}
	
	foreach singleEvent $events {
		unset -nocomplain event_type order_tag item_name additionals
		
		if {[dict get $singleEvent planned_start] < $to && [dict get $singleEvent planned_end] > $from} {
	
			dict with singleEvent {
				if {[info exists order_tag]} {
				
					if {![dict exists $singleEvent event_type]} {continue}
					if {[dict get $singleEvent event_type] ni [list 1 2]} {continue}
				
					if {$planned_start < $from} {set visible_start $from} {set visible_start $planned_start}
					if {$planned_end > $to} {set visible_end $to} {set visible_end $planned_end}
					# if {$evFrom < $current_time} {set evFrom $current_time}
					
					if {$planned_end > $current_time} {
						set extEvents [dict getnull $additionals EXTERNAL_TIME]
						#set extEventsSorted [lsort -integer -index 3 $extEvents]
						set extTimesInView [lsort -integer -index 3 [::rts_stats::getViewSessionExternal $extEvents $from $to]]

						#antLog extTimesInView $extTimesInView
						
						set prevOrderTs $visible_start
						
						foreach externalEvent $extTimesInView {
							set extFrom [dict get $externalEvent from]
							set extTo [dict get $externalEvent to]
							
							if {$extTo > $prevOrderTs} {
								dict lappend states work [list from $prevOrderTs to $extFrom]
								#dict lappend states external [list from $extFrom to $extTo]
								set prevOrderTs $extTo
							}
						}
						
						#Last external or order without external time
						if {$prevOrderTs < $visible_end} {
							dict lappend states work [list from $prevOrderTs to $visible_end]
							#if {$prevOrderTs > $current_time} {set partedPlannedTime [expr $partedPlannedTime + $work_time]}
						}
					}
				} else {
					if {$planned_start < $from} {set visible_start $from} {set visible_start $planned_start}
					if {$planned_end > $to} {set visible_end $to} {set visible_end $planned_end}
					if {$visible_start < $current_time} {set visible_start $current_time}
					lappend breaksEvents [list from $visible_start to $visible_end]
				}
			}
		}
	}
	
	if {[dict exists $states work]} {
		set sortedWorkStates [lsort -integer -index 1 [dict get $states work]]
	}
	#antLog states $states
	
	set breaksEvents [lsort -integer -index 1 $breaksEvents]
	set trimedEvents {}
	set prevStateTo {}

	foreach state $sortedWorkStates {
		set from [dict get $state from]
		set to [dict get $state to]
		
		if {$to > $prevStateTo} {
			if {$from < $prevStateTo && [string is integer $prevStateTo]} {set prevStateFinish $prevStateTo} {set prevStateFinish $from}
			lappend trimedEvents [list from $prevStateFinish to $to]
		}
		set prevStateTo $to
	}

	set mergedEvents {}
	
	if {[llength $trimedEvents] > 1} {
	   for {set i 0} {$i < [llength $trimedEvents]} {incr i} {

			set currEv [lindex $trimedEvents $i]
			set nextEv [lindex $trimedEvents [expr $i + 1]]
			
			set to [dict get $currEv to]
			
			if {[dict exists $nextEv from]} {
				if {[dict get $currEv to] == [dict get $nextEv from]} {
					set to [dict get $nextEv to]
					incr i
				}
			}
			
			lappend mergedEvents [list from [dict get $currEv from] to $to]
		}
	} else {
		set mergedEvents $trimedEvents
	}
	
	set sumWork 0
	foreach state $mergedEvents {
		set from [dict get $state from]
		set to [dict get $state to]

		set sumBreaks 0
		foreach breakEv $breaksEvents {
			set breakFrom [dict get $breakEv from]
			set breakTo [dict get $breakEv to]

			if {$breakFrom >= $from && $breakTo <= $to} {
				incr sumBreaks [expr $breakTo - $breakFrom]
			}
		}
		set sumWork [expr $sumWork + (($to - $from) - $sumBreaks)]
	}
	return $sumWork
}

proc ::rts_stats::calcOrderPac {stats singleEvent from to current_time} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	dict with singleEvent {
		if {$planned_start < $from} {set visible_start $from} {set visible_start $planned_start}
		if {$planned_end > $to} {set visible_end $to} {set visible_end $planned_end}
	
		if {![info exists item_name]} {set item_name ""}
		set visibleOrderTs [expr $visible_end - $visible_start]
		set fullOrderTs [expr $planned_end - $planned_start]

		set scaledOrderTarget 0
		set realOrderWork 0
		set breaksPoints {}
		#Planned orders and still realizing
		if {$planned_end > $current_time} {
			set extEvents [dict getnull $additionals EXTERNAL_TIME]
			set extEventsSorted [lsort -integer -index 3 $extEvents]
			set extTimesInView [lsort -integer -index 3 [::rts_stats::getViewSessionExternal $extEvents $from $to]]

			set partedTarget 0
			set partedPlannedTime 0
			set restPlannedOrderTs $visibleOrderTs
			#if {$visible_start < $current_time} {set visible_start $current_time}

			if {$planned_start < $from || $planned_end > $to} {
				set fullExtTs 0
				set prevOrderTs $planned_start
				foreach extEvent $extEventsSorted {
					if {[dict get $extEvent to] > $prevOrderTs} {
						incr fullExtTs [expr [dict get $extEvent to] - [dict get $extEvent from]]
						set prevOrderTs [dict get $extEvent to]
					}
				}
				set restPlannedOrderTs [expr $fullOrderTs - $fullExtTs]
			}

			set prevOrderTs $visible_start
			#antLog extTimesInView $extTimesInView
			
			foreach externalEvent $extTimesInView {
			
				set extFrom [dict get $externalEvent from]
				set extTo [dict get $externalEvent to]
				
				if {$extTo > $prevOrderTs} {
					set work_time [expr $extFrom - $prevOrderTs]
					if {$prevOrderTs > $current_time} {set partedPlannedTime [expr $partedPlannedTime + $work_time]}

					set partedTarget [expr $partedTarget + (1.00 * $work_time/$restPlannedOrderTs) * $target]
					lappend breaksPoints $extFrom $partedTarget

					set prevOrderTs $extTo
				}
			}
			
			#Last external or order without external time
			if {$prevOrderTs < $visible_end} {
				set work_time [expr $visible_end - $prevOrderTs]
				if {$prevOrderTs > $current_time} {set partedPlannedTime [expr $partedPlannedTime + $work_time]}

				set partedTarget [expr $partedTarget + (1.00 * $work_time/$restPlannedOrderTs) * $target]
				lappend breaksPoints $visible_end $partedTarget
			}

			#Set order target
			if {$planned_end < $to && $planned_start > $from} {
				set scaledOrderTarget $target
				set realOrderWork $partedPlannedTime
			} elseif {($planned_start < $from || $planned_end > $to) && $extEventsSorted eq {}} {
				set scaledOrderTarget [expr (1.00 * $visibleOrderTs / $fullOrderTs) * $target]
				set realOrderWork [expr $visible_end - $visible_start]
			} else {
				set scaledOrderTarget $partedTarget
				set realOrderWork $partedPlannedTime
			}
			
			set route_eff [dict getnull $::rts_stats::productsRouteInfo $item_tag $workcenter_tag EF]
			if {$route_eff ne {}} {
				antLog SUB_OEE_FACTOR_1 "item_tag $item_tag order_tag $order_tag route_eff $route_eff realOrderWork $realOrderWork"
				dict set stats OEE_FACTOR_1 [expr [dict get $stats OEE_FACTOR_1] + 1.0 * $realOrderWork * ($route_eff / 100.0)]
				dict set stats orders_time_future [expr [dict get $stats orders_time_future] + $realOrderWork]
				#dict set stats OEE_FACTOR_2 [expr $OEE_FACTOR_2 + $visibleOrderTs]
			}
			
		} else {
			#History
			if {$planned_end < $to && $planned_start > $from} {
				set scaledOrderTarget $target
			} else {
				if {[dict exists $stats hisDetails $order_tag]} {
					set hisDetails [dict get $stats hisDetails $order_tag]
					set scaledOrderTarget [expr (1.00 * ($visibleOrderTs - [dict get $hisDetails visExtTime]) / ($fullOrderTs - [dict get $hisDetails extTime])) * $target]
				} else {
					antLog warning "Order $order_tag not exists in history"
				}
			}
		}

      set packageVolume [dict getnull $::rts_stats::ordersPackageInfo $item_tag]

		if {$packageVolume != 0 && $packageVolume ne {}} {
			set packagesQuantity [expr 1.00 * $scaledOrderTarget / $packageVolume]
			dict set stats sum_target_op [expr [dict get $stats sum_target_op] + $packagesQuantity]
		}
	}
	
	#Prepare chart
	if {[dict get $stats producedChart] eq {} && $visibleOrderTs < [expr $to - $from]} {
		dict lappend stats producedChart [list 0 $from]
	}

	set orderChartLabel [join [list "&nbsp <br>" $item_name ""] ""]

	dict lappend stats targetChart [list [dict get $stats sum_target] $visible_start {} $orderChartLabel]

	foreach {ts currTarget} $breaksPoints {
		dict lappend stats targetChart [list [expr [dict get $stats sum_target] + $currTarget] $ts {} $orderChartLabel]
	}
	
	dict set stats sum_target [expr [dict get $stats sum_target] + $scaledOrderTarget]
	dict set stats orders_time [expr [dict get $stats orders_time] + $realOrderWork]

	dict lappend stats targetChart [list [dict get $stats sum_target] $visible_end {} $orderChartLabel]
	return $stats
}

proc ::rts_stats::calcOrderMan {stats singleEvent from to current_time} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	dict with singleEvent {
		set target [expr 1.00 * $target / 1000]
		set produced [expr 1.00 * $produced / 1000]
	
		if {$planned_start < $from} {set visible_start $from} {set visible_start $planned_start}
		if {$planned_end > $to} {set visible_end $to} {set visible_end $planned_end}
	
		if {![info exists item_name]} {set item_name ""}
		set visibleOrderTs [expr $visible_end - $visible_start]
		set fullOrderTs [expr $planned_end - $planned_start]

		set scaledOrderTarget 0
		set realOrderWork 0
		set breaksPoints {}
		#Planned orders and still realizing
		if {$planned_end > $current_time} {
			set extEvents [dict getnull $additionals EXTERNAL_TIME]
			set extEventsSorted [lsort -integer -index 3 $extEvents]
			set extTimesInView [lsort -integer -index 3 [::rts_stats::getViewSessionExternal $extEvents $from $to]]

			set partedTarget 0
			set partedPlannedTime 0
			set restPlannedOrderTs $visibleOrderTs
			#if {$visible_start < $current_time} {set visible_start $current_time}

			if {$planned_start < $from || $planned_end > $to} {
				set fullExtTs 0
				set prevOrderTs $planned_start
				foreach extEvent $extEventsSorted {
					if {[dict get $extEvent to] > $prevOrderTs} {
						incr fullExtTs [expr [dict get $extEvent to] - [dict get $extEvent from]]
						set prevOrderTs [dict get $extEvent to]
					}
				}
				set restPlannedOrderTs [expr $fullOrderTs - $fullExtTs]
			}

			set prevOrderTs $visible_start

			foreach externalEvent $extTimesInView {
			
				set extFrom [dict get $externalEvent from]
				set extTo [dict get $externalEvent to]
				
				if {$extTo > $prevOrderTs} {
					set work_time [expr $extFrom - $prevOrderTs]
					# if {$prevOrderTs > $current_time} {set partedPlannedTime [expr $partedPlannedTime + $work_time]}

					set partedTarget [expr $partedTarget + (1.00 * $work_time/$restPlannedOrderTs) * $target]
					lappend breaksPoints $extFrom $partedTarget

					set prevOrderTs $extTo
				}
			}
			
			#Last external or order without external time
			if {$prevOrderTs < $visible_end} {
				set work_time [expr $visible_end - $prevOrderTs]
				# if {$prevOrderTs > $current_time} {set partedPlannedTime [expr $partedPlannedTime + $work_time]}

				set partedTarget [expr $partedTarget + (1.00 * $work_time/$restPlannedOrderTs) * $target]
				lappend breaksPoints $visible_end $partedTarget
			}

			#Set order target
			if {$planned_end < $to && $planned_start > $from} {
				set scaledOrderTarget $target
				# set realOrderWork $partedPlannedTime
			} elseif {($planned_start < $from || $planned_end > $to) && $extEventsSorted eq {}} {
				set scaledOrderTarget [expr (1.00 * $visibleOrderTs / $fullOrderTs) * $target]
				# set realOrderWork [expr $visible_end - $visible_start]
			} else {
				set scaledOrderTarget $partedTarget
				# set realOrderWork $partedPlannedTime
			}
		} else {
			#History

			if {$planned_end < $to && $planned_start > $from} {
				set scaledOrderTarget $target
			} elseif {$workcenter_tag in {T_37 T_137 T_121 T_35}} {
			
				#set keyOrder [lindex [split $order_tag "_"] 0]
				set keyOrder $order_tag
				
				if {[dict exists hisDetails $keyOrder]} {
					set hisDetails [dict get $stats hisDetails $keyOrder]
					set scaledOrderTarget [expr (1.00 * ($visibleOrderTs - [dict get $hisDetails visExtTime]) / ($fullOrderTs - [dict get $hisDetails extTime])) * $target]
				} else {
					antLog warning "Order $order_tag not exists in history MANUFACTURING"
				}
			} elseif {$workcenter_tag in {APN G_01 G_02 D_25 D-25 D_20 D-20}} {
				set scaledOrderTarget [expr (1.00 * $visibleOrderTs / $fullOrderTs) * $target]
				# set realOrderWork [expr $visible_end - $visible_start]
			} else {
				set scaledOrderTarget $target
			}
		}
		
		#Prepare chart
		set orderChartLabel [join [list "&nbsp <br>" $item_name ""] ""]

		if {[dict get $stats producedChart] eq {} && $visibleOrderTs < [expr $to - $from]} {
			dict lappend stats producedChart [list 0 $from]
		}
		
		if {[dict get $stats orders_cnt] == 0} {
			if {$workcenter_tag ni {T_37 D_25 T_137 T_121 T_35}} {
				dict lappend stats producedChart [list [dict get $stats sum_production] $visible_start {} $orderChartLabel]
			}
			dict lappend stats targetChart [list [dict get $stats sum_target] $visible_start {} $orderChartLabel]
		}
		
		if {$produced > 0 && $planned_end < $to && $workcenter_tag ni {T_37 D_25 T_137 T_121 T_35}} {
			#IF EMPTY HIS?
			dict lappend stats producedChart [list [dict get $stats sum_production] $visible_start {} $orderChartLabel]
			dict set stats sum_production [expr [dict get $stats sum_production] + $produced]
			dict lappend stats producedChart [list [dict get $stats sum_production] $visible_end {} {}]
		}
	}
	
	dict set stats sum_target [expr [dict get $stats sum_target] + $scaledOrderTarget]
	return $stats
}

proc ::rts_stats::getViewSessionExternal {events from to} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	set newEvents {}
	foreach singleEvent $events {
		if {[dict get $singleEvent from] < $to && [dict get $singleEvent to] > $from} {
		
			if {[dict get $singleEvent from] < $from} {set visible_start $from} {set visible_start [dict get $singleEvent from]}
			if {[dict get $singleEvent to] > $to} {set visible_end $to} {set visible_end [dict get $singleEvent to]}
			
			dict set singleEvent from $visible_start
			dict set singleEvent to $visible_end
		
			lappend newEvents $singleEvent
		}
	}
	return $newEvents
}

proc ::rts_stats::setPageControls {stats_data selectedLine current_time session} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	if {[info exists ::EquipmentMap]} {set lineNames [concat {*}[dict values $::EquipmentMap]]}
	
	set from [dict get [DatePicker.getValue $session] dateValueFrom]
	set to [dict get [DatePicker.getValue $session] dateValueTo]
   set viewTs [expr $to - $from]
	set area [getSessionData $session DataServer area]
	set lang [dict get [avSessionInfo $session] lang]
	
	variable statesColours
	set stats_data [checkValues $stats_data]
	#Load data to controls
	dict with stats_data {

		lassign [::rts_stats::getFormatedOtherStats $otherStatsDetails] sumOther visStats

		set sumTime 0
      set sumOther [expr $sumOther + [expr $viewTs - ($sumOther + $orders_time + $setup_time)]]
      
		set sumTime [format %.2f [expr $orders_time + $setup_time + $sumOther]]

		if {$sumTime != 0} {
			set percentageScale [expr $sumTime / 100.00]

			StatesChart.setDictionaryChartSeriesData [list [list RTS_STATISTICS_PARTICIPATION_LABEL [list \
			[list [format %.0f [expr $orders_time.00/$percentageScale]] {RTS_STATISTICS_PRODUCTION_ORDER_LABEL} [dict get $statesColours orders_time]] \
			[list [format %.0f [expr $setup_time.00/$percentageScale]] {RTS_STATISTICS_CO_LABEL} [dict get $statesColours setup_time]] \
			[list [format %.0f [expr $sumOther.00/$percentageScale]] {RTS_STATISTICS_OTHERS_LABEL} [dict get $statesColours other_time]]]]] $session
		}

		ProductionLabel2.setDictionaryValue RTS_STATISTICS_TIME_ICON_LABEL [list [::planning::duration $orders_time]] $session
		ProductionLabel3.setDictionaryValue RTS_STATISTICS_ORDERS_COUNT_ICON_LABEL [list $orders_cnt] $session
		
		CoLabel2.setDictionaryValue RTS_STATISTICS_TIME_ICON_LABEL [list [::planning::duration $setup_time]] $session
		CoLabel3.setDictionaryValue RTS_STATISTICS_CO_COUNT_ICON_LABEL [list $setup_cnt] $session
		
		OtherLabel1.setDictionaryValue RTS_STATISTICS_OTHERS_LABEL $session
		OtherLabel2.setDictionaryValue RTS_STATISTICS_TIME_ICON_LABEL [list [::planning::duration $sumOther]] $session
		OtherLabel3.setDictionaryValue "<i class='fas fa-info fa-border' style='color: #0A78CC; cursor: pointer;' \
		onClick='[av::html::getCallbackFunction OtherDetails ::rts_stats::onInfoDetailsIcon]'></i>" $session
		
		OtherLabel4.setDictionaryValue "<ul>$visStats</ul>" $session
		
		if {[getSessionData $session DataServer isShowStatsDetails] eq {}} {OtherLabel4.hide $session}
		
		if {$area eq "MANUFACTURING"} {
			set prefixUnit ""
		} else {
			if {$lang eq "PL"} {
				set prefixUnit "tys."
			} elseif {$lang eq "EN"} {
				set prefixUnit "thous."
			}
		}
      
      set opProducedQuantity [format %.1f [expr $sum_production_op / 1000]]
      set opTargetQuantity [format %.1f [expr $sum_target_op / 1000]]
		set OEE_FACTOR_EXP $OEE_FACTOR_1
		
		if {$from > $current_time} {
			#set OEE_factor 0
			set OEE_FACTOR_EST [expr $OEE_FACTOR_2 + $setup_time_future + $orders_time_future]

			set ratio [format %.1f [expr $sum_target/1000.00]]

			CompareProductionLabel.setDictionaryValue RTS_STATISTICS_TARGET_CHART_LABEL [list $ratio $prefixUnit $units $opTargetQuantity $prefixUnit "op."] $session
			ProductionLabel1.setDictionaryValue RTS_STATISTICS_PRODUCTION_ORDER_PLANNED_LABEL $session
			CoLabel1.setDictionaryValue RTS_STATISTICS_CO_PLANNED_LABEL $session
			
		} else {
		
			if {$to > $current_time} {
				set OEE_FACTOR_EST [expr $OEE_FACTOR_2 + $setup_time_future + $orders_time_future]
			} else {
				set OEE_FACTOR_EST $OEE_FACTOR_2
			}

			set ratio "[format %.1f [expr $sum_production/1000.00]] / [format %.1f [expr $sum_target/1000.00]]"
			set packageRatio "$opProducedQuantity / $opTargetQuantity"
			
			CompareProductionLabel.setDictionaryValue RTS_STATISTICS_REALIZATED_CHART_LABEL [list $ratio $prefixUnit $units $packageRatio $prefixUnit "op."] $session
			ProductionLabel1.setDictionaryValue RTS_STATISTICS_PRODUCTION_ORDER_LABEL $session
			CoLabel1.setDictionaryValue RTS_STATISTICS_CO_LABEL $session
			
			if {$current_time < $to && $targetChart ne {}} {
				set RTSerie [list RT [list [list 0 $current_time] [list [expr 1.2 * [lindex [lindex $targetChart end] 0]] $current_time]] \
				[list 0 $to]]
			} else {
				CompareProductionChart.clearSeriesData $session
			}
		}
		
		if {$OEE_FACTOR_EST != 0} {
			set OEE_CALC [format %.2f [expr 100.00 * $OEE_FACTOR_EXP / $OEE_FACTOR_EST]]
		} else {
			set OEE_CALC 0
		}

      CompareProductionChart.removeSeries [list realization target RT] $session
		#CompareProductionChart.resetChartSeries $session
		CompareProductionChart.setYAxisTitle 0 "\[ $units \]" $session
		
		CompareProductionChart.addSeries [list \
		[list name realization serieLabel RTS_STATISTICS_REALIZATION_LABEL type area color rgba(124,181,236,0.5) precision 0 unit "$units"] \
		[list name target serieLabel RTS_STATISTICS_TARGET_LABEL type line color rgba(247,163,92,0.75) precision 0 unit "$units"] \
		[list name RT serieLabel RT type line lineStyle dash lineWidth 1 color red] \
		] $session

		set chartData [list [list realization $producedChart] [list target $targetChart]]
		if {[info exists RTSerie]} {lappend chartData $RTSerie}

		CompareProductionChart.setDictionaryChartSeriesData $chartData $session

		Factor1Progress.setValue $OEE_CALC $session
		Factor1Label.setDictionaryValue RTS_STATISTICS_OEE_FACTOR_LABEL $session
	}
}

proc ::rts_stats::getUnitPerLine { place_id args } {
   if {[antIsDebug]} {antLog debug [info level 0]}
	
   set opts [antParseOpts {
      -base 0
   } $args]
      
   set unitTag THOUSAND_BL_BTL
   if { "Polfa" == "RR" } {
      set unitTag THOUSAND_PCS
      set baseTag szt
   } else {
      set unitTag THOUSAND_TABLETS
      set baseTag UNIT_TABLETS
      if { $place_id in [list B_43 B_42 B_44] } {
         set unitTag THOUSAND_BLISTERS
         set baseTag UNIT_BLISTERS
      }
      if { $place_id in [list LB PDA] } {
         set unitTag THOUSAND_BOTTLES
         set baseTag UNIT_BOTTLES
      }
   }
   
   if { [dict get $opts -base] } {
      return $baseTag
   }
   return $unitTag 
}

proc ::rts_stats::calcOtherDetails {otherLineEvents otherTimeHistory} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	set durationOtherEvents {}

	dict for {statsTag data} $otherLineEvents {
		dict for {eventType eventsList} $data {
			foreach event $eventsList {

				if {$statsTag ne "CLIPBOARD"} {
					if {![dict exists $durationOtherEvents $eventType]} {dict set durationOtherEvents $eventType 0}
					dict set durationOtherEvents $eventType [expr [dict get $durationOtherEvents $eventType] + [dict get $event to] - [dict get $event from]]
				} else {
					if {![dict exists $durationOtherEvents [dict get $event name]]} {dict set durationOtherEvents [dict get $event name] 0}
					dict set durationOtherEvents [dict get $event name] [expr [dict get $durationOtherEvents [dict get $event name]] + [dict get $event to] - [dict get $event from]]
				}
			}
		}
	}

	if {[dict exists $durationOtherEvents PLANNED_DOWNTIME]} {
		dict set durationOtherEvents PLANNED_DOWNTIME [expr [dict get $durationOtherEvents PLANNED_DOWNTIME] + $otherTimeHistory]
	} else {
		dict set durationOtherEvents PLANNED_DOWNTIME $otherTimeHistory
	}

	return [lsort -integer -stride 2 -index 1 -decreasing $durationOtherEvents]
}

proc ::rts_stats::getFormatedOtherStats {otherStatsDetails} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	variable otherNames

	set sumOther 0
	set visStats {}
	dict for {eventTag Ts} $otherStatsDetails {
		if {[dict exists $otherNames $eventTag]} {set name [dict get $otherNames $eventTag]} {set name $eventTag}
		append visStats "<li> <span>[av::html::translate $name]</span> - [::planning::duration $Ts] </li>"
		
		set sumOther [expr $sumOther + $Ts]
	}
	return [list $sumOther $visStats]
}

proc ::rts_stats::onInfoDetailsIcon {session id} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	set isShowDetails [getSessionData $session DataServer isShowStatsDetails]
	
	if {$isShowDetails eq {} || $isShowDetails == 0} {
		OtherLabel4.show $session
		setSessionData $session DataServer isShowStatsDetails 1
	} else {
		OtherLabel4.hide $session
		setSessionData $session DataServer isShowStatsDetails 0
	}
}
 
proc ::rts_stats::getData {session} {

	set goodStaff {}
	set lackStaff {}
	set staffNeed {}
	set flag {}
	set flagStaff 0
	set flagEmpty 0
	set prevEndTime ""
	set timeFrom [antTimeToString [getSessionData $session DataServer timeFrom] $::utc 0]
	set timeTo [antTimeToString [getSessionData $session DataServer timeTo] $::utc 0]
	set sourceId [getSessionData $session DataServer sourceId]
	if {$sourceId == ""} {
		setSessionData $session DataServer sourceId 0
		set sourceId [getSessionData $session DataServer sourceId]
	}
	if {[getSessionData $session DataServer selectedRowId] eq ""} {
		set selectedLine 1
	} else {
		set selectedLine [lindex [split [getSessionData $session DataServer selectedRowId] "_"] 1]
	}
	set query " SELECT sw.place_id,sw.from_time,sw.to_time,sw.real_persons_s1,sw.real_persons_s2,persons_s1,
				  persons_s2,oa.planned_start,oa.planned_end
				  FROM (
					  SELECT swr.place_id,sw.from_time,sw.to_time,(case WHEN type_tag = 9 THEN count(value) ELSE NULL END) as real_persons_s1, (case WHEN type_tag = 8 THEN count(value) ELSE NULL END) as real_persons_s2
					  FROM Scheduling.Workers sw
					  LEFT JOIN Scheduling.WorkersResources swr
					  ON sw.resource_id = swr.id
					  WHERE sw.from_time >= '$timeFrom' AND sw.to_time <= '$timeTo' AND swr.type_tag IN (8,9)
					  GROUP BY swr.place_id, sw.from_time, sw.to_time, type_tag
				  ) sw
				  OUTER APPLY (
					  SELECT oa.order_tag,oa.equipment_id,oa.planned_start,oa.planned_end,lp.persons_s1,lp.persons_s2
					  FROM Planning.OrdersPlannedView oa
					  LEFT JOIN lines_performance lp
					  ON oa.item_id = lp.idx
					  WHERE oa.equipment_id = sw.place_id AND oa.planned_start <= sw.to_time AND oa.planned_end > sw.from_time and oa.source_id = $sourceId
				 ) oa
				 ORDER BY sw.from_time,oa.planned_start ASC"
	set result [utils::runSql $query]
	foreach row $result {
		foreach { place from_time to_time real_persons1 real_persons2 persons1 persons2 planned_start planned_finish} $row {
			if {$place eq $selectedLine} {
				 #brak danych w bazie
				#set persons1 2
				#set persons2 1
				####
				if {$real_persons1 == ""} {
					set real_persons1 0
				}
				if {$real_persons2 == ""} {
					set real_persons2 0
				}
				if {$persons1 == ""} {
					set persons1 0
				}
				if {$persons2 == ""} {
					set persons2 0
				}
				set sum_real [expr $real_persons1 + $real_persons2]
				set sum_planned [expr $persons1 + $persons2]

	#         if {[string range $from_time 11 12]=="04"} {
	#            set shift I
	#         } elseif {[string range $from_time 11 12]=="12"} {
	#            set shift II
	#         } else {
	#            set shift III
	#         }

				if {$planned_start==""} {
				 if {$prevEndTime != [antStringToTime $from_time $::utc] && $prevEndTime != ""} {
					  lappend staffNeed [list 0 $prevEndTime] [list 0 [antStringToTime $from_time $::utc]]
					  lappend lackStaff [list "null" $prevEndTime] [list "null" [antStringToTime $from_time $::utc]]
					  lappend goodStaff [list 0 $prevEndTime] [list 0 [antStringToTime $from_time $::utc]]
				 }
					lappend goodStaff [list $sum_real [antStringToTime $from_time $::utc]] [list $sum_real [antStringToTime $to_time $::utc]]
					lappend lackStaff [list "null" [antStringToTime $from_time $::utc]]
					lappend staffNeed [list 0 [antStringToTime $from_time $::utc]] [list 0 [antStringToTime $to_time $::utc]]
					set prevEndTime [antStringToTime $to_time $::utc]

				} else {
					if {$sum_real >= $sum_planned} {
						if {$real_persons1 >= $persons1} {
							if {$real_persons2 >= $persons2} {
								 set flag 1
							} else {
								set flag 0
							}
						} else {
							set flag 0
						}
					} else {
						set flag 0
					}
					set from [antStringToTime $from_time $::utc]
					set to [antStringToTime $to_time $::utc]
					set start [antStringToTime $planned_start $::utc]
					set finish [antStringToTime $planned_finish $::utc]
					#set flagStaff 0

					if {$from>$start} {
						set startTime $from
					} else {
						set startTime $start
					}

					if {$finish>$to} {
						set endTime $to
					} else {
						set endTime $finish
					}

					if {$flag==1} {
						 if {$prevEndTime != $startTime && $prevEndTime != ""} {
							lappend staffNeed [list 0 $prevEndTime] [list 0 $startTime]
							lappend lackStaff [list "null" $prevEndTime] [list "null" $startTime]
							lappend goodStaff [list 0 $prevEndTime] [list 0 $startTime]
						}
						lappend staffNeed [list $sum_planned $startTime] [list $sum_planned $endTime]
						lappend goodStaff [list $sum_real $startTime] [list $sum_real $endTime]
						lappend lackStaff [list "null" $startTime] [list "null" $endTime]
						set prevEndTime $endTime

					} else {
						if {$prevEndTime != $startTime && $prevEndTime != ""} {
							lappend staffNeed [list 0 $prevEndTime] [list 0 $startTime]
							lappend goodStaff [list "null" $prevEndTime] [list "null" $startTime]
							lappend lackStaff [list 0 $prevEndTime] [list 0 $startTime]
						}
						lappend staffNeed [list $sum_planned $startTime] [list $sum_planned $endTime]
						lappend goodStaff [list "null" $startTime] [list "null" $endTime]
						lappend lackStaff [list $sum_real $startTime] [list $sum_real $endTime]
						set prevEndTime $endTime
					}
				}
			}    
		}
	}
	set staffNeed [lsort -index 1 $staffNeed]
	set goodStaff [lsort -index 1 $goodStaff]
	set lackStaff [lsort -index 1 $lackStaff]
	set newStaffNeed ""
	set prevLinRow0 ""
	set prevLinRow1 ""
	#usuwanie powtarzających się czasów dla tej samej liczby potrzebnych pracowników
	foreach row $staffNeed {
		if {[lindex $row 0] == $prevLinRow0 && [lindex $row 1] == $prevLinRow1} {

		} else {
			lappend newStaffNeed $row
		}
		set prevLinRow0 [lindex $row 0]
		set prevLinRow1 [lindex $row 1]
	}
	#antLog "$newStaffNeed $goodStaff $lackStaff"


	ResourceChart.setSeriesData $session [list [list staffNeed $newStaffNeed] [list goodStaff $goodStaff] [list lackStaff $lackStaff]]
	ResourceChart.setXAxisRange [getSessionData $session DataServer timeFrom] [getSessionData $session DataServer timeTo] "" $session
}

proc ::rts_stats::checkValues {data} {

	set dataChecked $data

	foreach {key val} $dataChecked {
		if {[llength $val] == 1 && $val < 0} {
			dict set dataChecked $key 0
		} elseif {[llength $val] > 0 && $key in [list targetChart productionChart]} {

			set prevChartPoint {}

			foreach chartPoint $val {
				lassign $chartPoint val Ts
				lassign $prevChartPoint prevVal prevTs

				if {$prevChartPoint ne {}} {
					if {$Ts < $prevTs || $val < $prevVal} {
						dict set dataChecked $key {}
						break
					}
				}
				set prevChartPoint $chartPoint
			}
		}
	}
	return $dataChecked
}

proc ::planning::duration { int_time } {
	set timeList [list]
	
	if {$int_time >= 86400} {
		
		foreach div {86400 3600} mod {0 24} name {d h} {
			set n [expr {$int_time / $div}]
			if {$mod > 0} {set n [expr {$n % $mod}]}
			
			if {$n == 1 && [llength $timeList] == 0} {
				 lappend timeList "$n d"
			} elseif {$n > 1} {
				 lappend timeList "$n ${name}"
			}
		}
		
	} else {
		set h [expr $int_time / 3600]
		set min [expr $int_time / 60]
		
		if {$h == 0 && $min == 0} {return 0}
		
		if {$h > 0} {
			lappend timeList "$h h"
		}
		if {$min > 0 && $min < 60} {
			lappend timeList "$min m"	
		}
	}
	
	return [join $timeList]
}

proc setPageProfit {session} {
	if {[antIsDebug]} {antLog debug [info level 0]}
	
	set currTarget [getSessionData $session DataServer completeTargetStats]
	set targetFreezed [getSessionData $session DataServer completeTargetStatsFreezed]
	
	set difPcsIcon {}
	set difOpIcon {}
	
	if {[dict exists $currTarget sumVisLinesTarget] && [dict exists $targetFreezed sumVisLinesTarget]} {
		set difPcs [expr 1.0 * ([dict get $currTarget sumVisLinesTarget] - [dict get $targetFreezed sumVisLinesTarget]) / 1000]
		set difOp [expr 1.0 * ([dict get $currTarget sumVisLinesTargetOp] - [dict get $targetFreezed sumVisLinesTargetOp]) / 1000]
		
		if {$difPcs > 0} {
			set difPcsIcon "<div style='float:right; color:#007f09;'>&nbsp(<i class='fa fa-caret-up'></i> [format %.1f $difPcs])</div>"
			set difOpIcon "<div style='float:right; color:#007f09;'>&nbsp(<i class='fa fa-caret-up'></i> [format %.1f $difOp])</div>"
		} elseif {$difPcs < 0} {
			set difPcsIcon "<div style='float:right; color:#e24c4b;'>&nbsp(<i class='fa fa-caret-down'></i> [format %.1f $difPcs])</div>"
			set difOpIcon "<div style='float:right; color:#e24c4b;'>&nbsp(<i class='fa fa-caret-down'></i> [format %.1f $difOp])</div>"
		} else {
			set difPcsIcon {}
			set difOpIcon {}
		}
	}
	
	InfoStats.setValue "<div style='font-weight: 14px;text-align: left; margin-left: 10px; margin-right: 10px;'><br><p style='border-bottom:1px solid #d3d3d3;'>Cel sumaryczny:</p>
		<div style='padding-left: 10px;'>[format %0.2f [expr 1.0 * [dict getnull $currTarget sumVisLinesTarget] / 1000]] tys. szt $difPcsIcon</div>
		<div style='padding-left: 10px;'>[format %0.2f [expr 1.0 * [dict getnull $currTarget sumVisLinesTargetOp] / 1000]] tys. op. $difOpIcon</div>
	</div>" $session
}

proc ::rts_stats::getUnit { line } {
	if {[antIsDebug]} {antLog debug [info level 0]}
	set tagLine [join [lrange [split $line _] 1 end] "_"]

	if { $tagLine in {LB PDA} } {
		set unit "btl"
	} elseif {$tagLine in {B_43 B_42 B_44}} {
		set unit "bl."
	} else {
		set unit "TT"
	}
	return $unit
}
import type { Position } from '../api/topology'

export interface PortAnchor {side:'left'|'right';offset:number}
export interface EdgeAnchors {source:PortAnchor;target:PortAnchor}
export interface LayoutPlan {
 positions:Record<string,Position>
 ports:Record<string,EdgeAnchors>
 /** Full orthogonal paths including both absolute port endpoints. */
 routes:Record<string,Position[]>
 nodeWidth:number
 nodeHeight:number
}
export interface RoutingUpdate {
 positions:Record<string,Position>
 routes:Record<string,Position[]>
 changedNodeIds:string[]
 changedEdgeIds:string[]
 adjusted:boolean
}

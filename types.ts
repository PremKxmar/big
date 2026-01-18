export interface TrafficStats {
  totalVehicles: number;
  avgCongestion: number;
  avgSpeed: number;
  mlAccuracy: number;
}

export interface Prediction {
  time: string;
  congestionLevel: number; // 0-100
  zone: string;
}

export interface Hotspot {
  id: string;
  name: string;
  congestion: number;
  coordinates: [number, number];
  trend: 'up' | 'down' | 'stable';
}

export interface TrafficEvent {
  id: string;
  lat: number;
  lng: number;
  speed: number;
  type: 'taxi' | 'bus' | 'private';
}

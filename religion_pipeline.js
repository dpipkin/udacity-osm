var pipeline = [
  { $match: {
    amenity: 'place_of_worship'
  } },
  { $lookup: {
    from: 'dirty_nodes',
    localField: '_id',
    foreignField: 'id',
    as: 'dirty_node' 
  } },
  { $addFields: {
    religion: { $arrayElemAt: ['$dirty_node.religion', 0] },
    denomination: { $arrayElemAt: ['$dirty_node.denomination', 0] }
  } },
  { $project: {
    dirty_node: 0
  }},
  { $out: 'with_churches' }
]

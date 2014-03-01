package DeltaBinaryPacking;



public interface TypeVisitor {
	  /**
	   * @param groupType the group type to visit
	   */
	  //void visit(GroupType groupType);

	  /**
	   * @param messageType the message type to visit
	   */
	//  void visit(MessageType messageType);

	  /**
	   * @param primitiveType the primitive type to visit
	   */
	  void visit(PrimitiveType primitiveType);

	}
